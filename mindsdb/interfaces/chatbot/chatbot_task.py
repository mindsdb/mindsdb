import pandas as pd

from mindsdb_sql.parser.ast import *

class ChatBotTask:
    def __init__(self, session, database, project_name, model_name, params):

        self.session = session

        self.model_name = model_name
        self.db_handler = self.session.integration_controller.get_handler(database)
        self.project_datanode = self.session.datahub.get(project_name)

        self.params = {
            'polling': {
                'type': 'message_count',
                'table': 'directs',
                'chat_id_col': '_id',
                'count_col': 'msgs'
            },
            'chat_table': {
                'name': 'direct_messages',
                'chat_id_col': 'room_id',
                'username_col': 'username',
                'text_col': 'text',
            },
            'bot_name': 'Andrey',
        }
        if params is not None:
            self.params.update(params)

        self.chats_prev = None


        # todo get params from handler


        # self.db_handler = self.session.datahub.get(self.database)

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

        # get chats status info
        ast_query = Select(
            targets=[Identifier(p_params['chat_id_col']), Identifier(p_params['count_col'])],
            from_table=Identifier(p_params['table'])
        )

        data, _ = self.db_handler.query(
            query=ast_query,
            session=self.session
        )

        chats = {}
        for chat_id, msgs in data:
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
        bot_username = self.params['bot_name']

        t_params = self.params['chat_table']

        ast_query = Select(
            targets=[Identifier(t_params['text_col'], alias=Identifier('text')),
                     Identifier(t_params['username_col'], alias=Identifier('username'))],
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
        if df[0]['username'] == bot_username:
            # the last message is from bot
            return

        messages = []
        for _, row in df.iterrows():
            if row['username'] == bot_username:
                prefix = 'assistant'
            else:
                prefix = 'user'
            messages.append(f"{prefix} :{row['text']}")

        data = [{
            'user_input': '\n'.join(messages)
        }]

        # call model
        predictions = self.project_datanode.predict(
            model_name=self.model_name,
            data=data,
        )

        model_response = predictions[0][answer_column]

        # send answer to user
        ast_query = Insert(
            table=Identifier(t_params['name']),
            columns=[t_params['chat_id_col'], t_params['text_col']],
            values=[
                chat_id, model_response
            ]
        )

        resp = self.db_handler.query(ast_query)