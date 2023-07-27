import datetime as dt
from collections import defaultdict

from mindsdb_sql.parser.ast import Identifier, Select, Insert, BinaryOperation, Constant
from mindsdb.interfaces.storage import db

from mindsdb.integrations.libs.api_handler import APIChatHandler


class BotException(Exception):
    pass


class ChatBotTask:
    def __init__(self, session, database, project_name, model_name, params):

        self.session = session
        self.project_name = project_name

        self.db_handler = self.session.integration_controller.get_handler(database)
        self.project_datanode = self.session.datahub.get(project_name)

        # get chat handler info
        self.params = {}
        if isinstance(self.db_handler, APIChatHandler):
            self.params = self.db_handler.get_chat_config()

            # get bot username
            self.params['bot_username'] = self.db_handler.get_my_user_name()

        if params is not None:
            self.params.update(params)

        # previous chats information
        self.chats_prev = None

        # get model info
        self.params['model'] = self._get_model(model_name)

        self.chat_memory = defaultdict(dict)

        self.params['back_db'] = {}
        back_db = self.params.get('backoffice_db')
        if back_db is not None:
            self.back_db = self.session.integration_controller.get_handler(back_db)

            if hasattr(self.back_db, 'back_office_config'):
                self.params['back_db']['config'] = self.back_db.back_office_config()

            if 'tools' in self.params['back_db']['config']:
                tools = [
                    {
                        'name': name,
                        'func': getattr(self.back_db, name),
                        'description': description
                    }
                    for name, description in self.params['back_db']['config']['tools'].items()
                ]
                self.params['back_db']['tools'] = tools

    def _get_model(self, model_name):
        model = self.session.model_controller.get_model(model_name, project_name=self.project_name)
        model_record = db.Predictor.query.get(model['id'])
        integration_record = db.Integration.query.get(model_record.integration_id)

        return {
            'model_name': model_name,
            'user_column': model_record.learn_args['using']['user_column'],
            'bot_column': model_record.learn_args['using']['assistant_column'],
            'output': model_record.to_predict[0],
            'engine': integration_record.engine,
        }

    def run(self):

        # get chat ids to react
        if self.params['polling']['type'] == 'message_count':
            chat_ids = self.get_chats_by_message_count()
        else:
            raise NotImplementedError

        for chat_id in chat_ids:
            self.answer_to_user(chat_id)

    def get_chats_by_message_count(self):

        p_params = self.params['polling']

        chat_ids = []

        id_col = p_params['chat_id_col']
        msgs_col = p_params['count_col']
        # get chats status info
        ast_query = Select(
            targets=[
                Identifier(id_col),
                Identifier(msgs_col)],
            from_table=Identifier(p_params['table'])
        )

        resp = self.db_handler.query(query=ast_query)
        if resp.data_frame is None:
            raise BotException('Error to get count of messages')

        chats = {}
        for row in resp.data_frame.to_dict('records'):
            chat_id = row[id_col]
            msgs = row[msgs_col]

            chats[chat_id] = msgs

        if self.chats_prev is None:
            # first run
            self.chats_prev = chats
        else:
            # compare
            # for new keys
            for chat_id, count_msgs in chats.items():
                if self.chats_prev.get(chat_id) != count_msgs:
                    chat_ids.append(chat_id)

            self.chats_prev = chats
        return chat_ids

    def answer_to_user(self, chat_id):
        bot_username = self.params['bot_username']

        t_params = self.params['chat_table']

        text_col = t_params['text_col']
        username_col = t_params['username_col']
        time_col = t_params['time_col']

        ast_query = Select(
            targets=[Identifier(text_col),
                     Identifier(username_col),
                     Identifier(time_col)],
            from_table=Identifier(t_params['name']),
            where=BinaryOperation(
                op='=',
                args=[
                    Identifier(t_params['chat_id_col']),
                    Constant(chat_id)
                ]
            )
        )

        resp = self.db_handler.query(ast_query)
        if resp.data_frame is None:
            raise Exception('Error to get list of messages')

        df = resp.data_frame

        # check first message:
        if len(df) == 0:
            return
        if df[username_col][0] == bot_username:
            # the last message is from bot
            return

        df = df.rename(columns={
            text_col: 'text',
            username_col: 'username',
            time_col: 'sent_at'
        })
        model_output = self.apply_model(df, chat_id)

        # send answer to user
        ast_query = Insert(
            table=Identifier(t_params['name']),
            columns=[t_params['chat_id_col'], t_params['text_col']],
            values=[
                [chat_id, model_output],
            ]
        )

        self.db_handler.query(ast_query)

    def _chat_history_to_conversation(self, df, chat_id):

        bot_username = self.params['bot_username']

        question_col = self.params['model']['user_column']
        answer_col = self.params['model']['bot_column']

        history_from = self.chat_memory[chat_id].get('history_from')
        if history_from:
            df = df[df.sent_at >= history_from]

        messages = []
        msgs = df.to_dict('records')

        for row in msgs:
            text = row['text']

            if text is None or text.strip() == '':
                # skip empty rows
                continue

            if row['username'] != bot_username:
                # create new message row
                messages.append({question_col: text, answer_col: None})
            else:
                if len(messages) == 0:
                    # add empty row
                    messages.append({question_col: None, answer_col: None})

                # update answer in previous column
                messages[-1][answer_col] = text
        return messages

    def _make_mode_prompt(self, chat_id):
        # choose prompt or model depending on mode
        mode_name = self.chat_memory[chat_id].get('mode')

        prompt = None
        model_info = None
        allowed_tools = None

        if mode_name is None:
            # mode in not selected, lets to go to select menu
            prompt = self._make_select_mode_prompt()
        else:
            # mode is selected

            mode = self.params['modes'].get(mode_name)
            if mode is None:
                # wrong mode
                self.chat_memory[chat_id]['mode'] = None
                raise BotException(f'Error to use mode: {mode_name}')

            if 'model' in mode:
                # this is model
                model_info = self._get_model(mode['model'])

            elif 'prompt' in mode:
                # it is just a prompt. let's use a bot model and custom prompt
                prompt = mode['prompt']

            else:
                raise BotException(f'Mode is not supported: {mode}')

            allowed_tools = mode.get('allowed_tools')

        return prompt, model_info, allowed_tools

    def _get_avail_modes_items(self):
        return [
            f'- code: {key}, description: {value["info"]}'
            for key, value in self.params.get('modes', {}).items()
        ]

    def _make_select_mode_prompt(self):
        # select mode tool
        task_items = self._get_avail_modes_items()

        tasks = '\n'.join(task_items)

        prompt = f'You are a helpful assistant and you can help with various types of tasks.' \
                 f'\nAvailable types of tasks:' \
                 f'\n{tasks}' \
                 f'\nUser have to choose task and assistant MUST call select_task function after it'

        return prompt

    def apply_model(self, df, chat_id):
        # sort by time
        df = df.sort_values(by='sent_at')

        model_info = self.params['model']
        prompt = None
        tools = None
        allowed_tools = None

        switched_to_mode = []

        def _select_task(mode_name):
            if mode_name == '':
                self.chat_memory[chat_id]['mode'] = None
                switched_to_mode.append(None)
                return 'success'

            avail_modes = list(self.params['modes'].keys())
            if mode_name not in avail_modes:
                return f'Error: task is not found. Available tasks: {", ".join(avail_modes)}'
            self.chat_memory[chat_id]['mode'] = mode_name
            switched_to_mode.append(mode_name)
            return 'success'

        select_task_tool = {
            'name': 'select_task',
            'func': _select_task,
            'description': 'Have to be used by assistant to select task. Input is task type. '
                           'If user want to unselect task input should be empty string. '
                           'Available tasks: ' + '; '.join(self._get_avail_modes_items())
        }

        if self.params.get('modes') is not None:
            # a bot with modes
            prompt, model_info2, allowed_tools = self._make_mode_prompt(chat_id)
            if model_info2 is not None:
                model_info = model_info2

            # workaround: don't show history if mode is not selected, otherwise bot doesn't decide to change mode
            if self.chat_memory[chat_id].get('mode') is None:
                df = df[-1:]

        if model_info['engine'] == 'langchain':
            tools = []
            for tool in self.params['back_db']['tools']:
                if allowed_tools is None or tool['name'] in allowed_tools:
                    tools.append(tool)

            if self.params.get('modes') is not None:
                # add mode tool
                tools.append(select_task_tool)

        context_list = [
            f"- Today's date is {dt.datetime.now().strftime('%Y-%m-%d')}."
            f" It must be used to understand the input date from string like 'tomorrow', 'today', 'yesterday'"
        ]
        context = '\n'.join(context_list)

        # call model
        params = {'tools': tools, 'context': context, 'max_iterations': 10, 'prompt': prompt}

        messages = self._chat_history_to_conversation(df, chat_id)

        predictions = self.project_datanode.predict(
            model_name=model_info['model_name'],
            data=messages,
            params=params
        )

        if len(switched_to_mode) > 0:
            # mode changed:
            # - clear previous history
            # - run once again

            # start conversation only from last message
            self.chat_memory[chat_id]['history_from'] = df.iloc[-1]['sent_at']

            messages = self._chat_history_to_conversation(df, chat_id)

            prompt, model_info2, allowed_tools = self._make_mode_prompt(chat_id)

            params['tools'] = []
            for tool in self.params['back_db']['tools']:
                if allowed_tools is None or tool['name'] in allowed_tools:
                    params['tools'].append(tool)

            params['prompt'] = prompt
            if model_info2 is not None:
                model_info = model_info2

            predictions = self.project_datanode.predict(
                model_name=model_info['model_name'],
                data=messages,
                params=params
            )

        output_col = model_info['output']
        model_output = predictions.iloc[-1][output_col]
        return model_output
