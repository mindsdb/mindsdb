import datetime as dt
import json
import re
from collections import defaultdict
from mindsdb_sql.parser.ast import Identifier, Select, Insert, BinaryOperation, Constant
from mindsdb.interfaces.storage import db

from mindsdb.integrations.libs.api_handler import APIChatHandler


class ChatBotTask:
    def __init__(self, session, database, project_name, model_name, params):

        self.session = session

        self.model_name = model_name
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

        self.chats_prev = None

        # get model info
        model = self.session.model_controller.get_model(model_name, project_name=project_name)
        model_record = db.Predictor.query.get(model['id'])

        self.params['model'] = {
            'user_column': model_record.learn_args['using']['user_column'],
            'bot_column': model_record.learn_args['using']['assistant_column'],
            'prompt': model_record.learn_args['using']['prompt'],
            'output': model_record.to_predict[0]
        }

        self.back_db_config = {}
        back_db = self.params.get('backoffice_db')
        if back_db is not None:
            self.back_db = self.session.integration_controller.get_handler(back_db)

            if hasattr(self.back_db, 'back_office_config'):
                self.back_db_config = self.back_db.back_office_config()

        self.user_memory = defaultdict(list)

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
            raise Exception()

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

        ast_query = Select(
            targets=[Identifier(text_col),
                     Identifier(username_col)],
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
            raise Exception()

        df = resp.data_frame

        # check first message:
        if len(df) == 0:
            return
        if df[username_col][0] == bot_username:
            # the last message is from bot
            return

        question_col = self.params['model']['user_column']
        answer_col = self.params['model']['bot_column']

        messages = []
        msgs = df.to_dict('records')
        # sort by time
        msgs.reverse()
        for row in msgs:
            text = row[text_col]

            if text is None or text.strip() == '':
                # skip empty rows
                continue

            if row[username_col] != bot_username:
                # create new message row
                messages.append({question_col: text, answer_col: None})
            else:
                if len(messages) == 0:
                    # add empty row
                    messages.append({question_col: None, answer_col: None})

                # update answer in previous column
                messages[-1][answer_col] = text

        model_output = self.apply_tool_model(messages, chat_id)

        # send answer to user
        ast_query = Insert(
            table=Identifier(t_params['name']),
            columns=[t_params['chat_id_col'], t_params['text_col']],
            values=[
                [chat_id, model_output],
            ]
        )

        self.db_handler.query(ast_query)

    def apply_model(self, messages, chat_id):
        # call model
        predictions = self.project_datanode.predict(
            model_name=self.model_name,
            data=messages,
        )

        output_col = self.params['model']['output']
        model_output = predictions.iloc[-1][output_col]
        return model_output

    def make_prompt(self, chat_id):
        chatbot_prompt_template = '''
You are assistant. You ether can speak to user directly or use a tool.

{model_prompt}

Possible user requests:
{options_list}

TOOLS:

Assistant is authorized to use the following tools to get addition information.
If you is using tool you must not not communicate with user. Don't ask the user to use it. Don't use more than one tool at once
List of tools:
{tools_list}

To use a tool, please use the following format:
```
<tool>tool name</tool>
<input>tool input</input>
```

Useful information:
{context_list}

History of using tools:
{tools_history}
        '''

        model_prompt = self.params['model']['prompt']

        options_list = []
        num = 0
        for tool, rules in self.back_db_config['options'].items():
            num += 1
            options_list.append(f'{num}. {tool}\n{rules}')

        options = '\n\n'.join(options_list)

        tools_list = []
        num = 0
        for tool, rules in self.back_db_config['tools'].items():
            num += 1
            tools_list.append(f'{num}. {tool}\n{rules}')

        tools = '\n\n'.join(tools_list)

        context_list = [
            f"- Today's date is {dt.datetime.now().strftime('%Y-%m-%d')}"
        ]
        context = '\n'.join(context_list)

        tools_history_list = []
        for item in self.user_memory[chat_id]:
            tools_history_list.append(f'<tool>{item["tool"]}</tool>\n'
                                      f'<input>{item["input"]}</input>\n'
                                      f'<result>{item["result"]}</result>')
        tools_history = '\n\n'.join(tools_history_list)

        prompt = chatbot_prompt_template.format(model_prompt=model_prompt, options_list=options,
                                                tools_list=tools, context_list=context, tools_history=tools_history)



        return prompt

    def apply_tool_model(self, messages, chat_id):


        output_col = self.params['model']['output']
        # question_col = self.params['model']['user_column']
        # answer_col = self.params['model']['bot_column']

        # append previous model responses
        # system_responses = []
        # for i, message in enumerate(messages):
        #     question = str(message)
        #     if question and question in self.user_memory[chat_id]:
        #         system_responses.append([i, self.user_memory[chat_id][question]])
        # # inject
        # system_responses.sort(key=lambda x: x[0], reverse=True)
        # for idx, message in system_responses:
        #     messages.insert(idx + 1, message)

        iteration = 0
        while iteration < 5:
            iteration += 1

            prompt = self.make_prompt(chat_id)

            # call model
            predictions = self.project_datanode.predict(
                model_name=self.model_name,
                data=messages,
                params={'prompt': prompt}
            )

            model_output = predictions.iloc[-1][output_col]

            system_messages = []
            if '<tool>' in model_output:
                tools = re.findall(r'<tool>([_\da-zA-Z]+)</tool>', model_output)
                inputs = re.findall('<input>([^<]+)</input>', model_output)
                if len(tools) > 0 and len(inputs) > 0:
                    tool = tools[0]
                    tool_input = inputs[0]
                    # does it json?
                    try:
                        tool_input = json.loads(tool_input)
                    except Exception:
                        pass

                    method = getattr(self.back_db, tool)
                    try:
                        resp = method(tool_input)
                    except:
                        print('ERROR')
                        resp = 'error'
                    system_response = str(resp)

                    # add to conversation and repeat model call
                    self.user_memory[chat_id].append(
                        {
                            'tool': tool,
                            'input': tool_input,
                            'result': system_response,
                        }
                    )

                    continue

            # no actions, exit
            break

        return model_output
