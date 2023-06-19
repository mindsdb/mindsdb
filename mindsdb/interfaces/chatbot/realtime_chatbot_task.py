from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.chatbot.chatbot_message import ChatBotMessage
from mindsdb.utilities.context import context as ctx

class RealtimeChatBotTask:
    """
    Manages the lifecycle and operation of a single chatbot.
    """
    
    _DEFAULT_MAX_ITERATIONS = 10
    _PROMPT_USER_COLUMN = 'input'

    def __init__(self, handler_factory, chat_engine, bot_record):
        
        # Need to set context first.
        self._bot_record = bot_record
        self._set_context()
        self._session = SessionController()

        self._chat_handler = handler_factory.create_realtime_chat_handler(chat_engine, self._on_message, bot_record.params)

        self._model_name = self._bot_record.model_name
        project_name = db.Project.query.get(self._bot_record.project_id).name
        self._project_datanode = self._session.datahub.get(project_name)

        # Get model info.
        model = self._session.model_controller.get_model(self._model_name, project_name=project_name)
        model_record = db.Predictor.query.get(model['id'])

        # Configure columns to use for responding.
        self._user_col = RealtimeChatBotTask._PROMPT_USER_COLUMN
        if 'using' in model_record.learn_args and 'user_column' in model_record.learn_args['using']:
            self._user_col = model_record.learn_args['using']['user_column'],
        self._output_col = model_record.to_predict[0]

    def _set_context(self):
        ctx.set_default()
        ctx.company_id = self._bot_record.company_id
        if self._bot_record.user_class is not None:
            ctx.user_class = self._bot_record.user_class

    def run(self):
        """Begins listening and responding to messages"""
        self._chat_handler.connect()

    def stop(self):
        """Stops listening and responding to messages"""
        self._chat_handler.disconnect()

    def _on_message(self, message: ChatBotMessage):
        self._set_context()
        received_history = db.ChatBotsHistory(
            chat_bot_id=self._bot_record.id,
            type=message.type.name,
            text=message.text,
            user=message.user,
            destination='bot',
        )
        db.session.add(received_history)

        predict_params = {
            'max_iterations': RealtimeChatBotTask._DEFAULT_MAX_ITERATIONS,
            'verbose': True,
            'tools': []
        }
        if message.type == ChatBotMessage.Type.DIRECT:
            chatbot_response = self._project_datanode.predict(
                model_name=self._model_name,
                data={ self._user_col: message.text },
                params=predict_params
            )

            response_text = chatbot_response.iloc[-1][self._output_col]
            response_message = ChatBotMessage(
                ChatBotMessage.Type.DIRECT,
                response_text,
                None,
                message.user
            )
            response = self._chat_handler.send_message(response_message)
            reply_history = db.ChatBotsHistory(
                chat_bot_id=self._bot_record.id,
                type=ChatBotMessage.Type.DIRECT.name,
                text=response_text,
                user='bot',
                destination=message.user,
            )
            if response.error:
                reply_history.error = response.error
            db.session.add(reply_history)
            db.session.commit()
            return
        
        raise NotImplementedError('Realtime chatbots only support direct messages currently')
