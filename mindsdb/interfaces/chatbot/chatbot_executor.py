from .model_executor import ModelExecutor
from .types import Function, BotException


class BotExecutor:
    def __init__(self, chat_task, chat_memory):
        self.chat_task = chat_task
        self.chat_memory = chat_memory

    def _get_model(self, model_name):
        return ModelExecutor(self.chat_task, model_name)

    def _prepare_available_functions(self):

        # collecting functions
        functions = []

        back_db_name = self.chat_task.bot_params.get('backoffice_db')
        if back_db_name is not None:
            back_db = self.chat_task.session.integration_controller.get_handler(back_db_name)
            if hasattr(back_db, 'back_office_config'):
                back_db_config = back_db.back_office_config()

                for name, description in back_db_config.get('tools', {}).items():
                    functions.append(
                        Function(
                            name=name,
                            description=description,
                            callback=getattr(back_db, name)
                        ))
        return functions

    def process(self):
        functions = self._prepare_available_functions()

        model_executor = self._get_model(self.chat_task.base_model_name)
        model_output = model_executor.call(self.chat_memory.get_history(), functions)
        return model_output


class MultiModeBotExecutor(BotExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._modes = self.chat_task.bot_params['modes']

    def _get_avail_modes_items(self):
        return [
            f'- code: {key}, description: {value["info"]}'
            for key, value in self._modes.items()
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

    def enter_bot_mode(self, functions):
        # choose prompt or model depending on mode
        mode_name = self.chat_memory.get_mode()

        allowed_tools = None

        if mode_name is None:
            # mode in not selected, lets to go to select menu
            model_executor = self._get_model(self.chat_task.base_model_name)
            prompt = self._make_select_mode_prompt()

            model_executor.prompt = prompt

        else:
            # mode is selected
            mode = self._modes.get(mode_name)
            if mode is None:
                # wrong mode
                self.chat_memory.set_mode(None)
                raise BotException(f'Error to use mode: {mode_name}')

            if 'model' in mode:
                # this is model
                model_executor = self._get_model(mode['model'])

            elif 'prompt' in mode:
                # it is just a prompt. let's use a bot model and custom prompt
                model_executor = self._get_model(self.chat_task.base_model_name)
                model_executor.prompt = mode['prompt']

            else:
                raise BotException(f'Mode is not supported: {mode}')

            allowed_tools = mode.get('allowed_tools')

        if allowed_tools is not None:
            functions = [
                fnc
                for fnc in functions
                if fnc.name in allowed_tools
            ]

        return model_executor, functions

    def _mode_switching_function(self, switched_to_mode):
        # add mode tool

        def _select_task(mode_name):
            if mode_name == '':
                self.chat_memory.set_mode(None)
                switched_to_mode.append(None)
                return 'success'

            avail_modes = list(self._modes.keys())
            if mode_name not in avail_modes:
                return f'Error: task is not found. Available tasks: {", ".join(avail_modes)}'
            self.chat_memory.set_mode(mode_name)
            switched_to_mode.append(mode_name)
            return 'success'

        return Function(
            name='select_task',
            callback=_select_task,
            description='Have to be used by assistant to select task. Input is task type.'
                        ' If user want to unselect task input should be empty string.'
                        ' Available tasks: ' + '; '.join(self._get_avail_modes_items())
        )

    def process(self):
        # this list should be changed if mode was switched
        switched_to_mode = []

        functions_all = self._prepare_available_functions()

        # Modes handling
        functions_all.append(self._mode_switching_function(switched_to_mode))

        model_executor, functions = self.enter_bot_mode(functions_all)

        # workaround: don't show history if mode is not selected, otherwise bot doesn't decide to change mode
        if self.chat_memory.get_mode() is None:
            self.chat_memory.hide_history(left_count=1)

        model_output = model_executor.call(self.chat_memory.get_history(), functions)

        if len(switched_to_mode) > 0:
            # mode changed:
            # - clear previous history
            # - run once again

            # start conversation only from last message
            self.chat_memory.hide_history(left_count=1)

            model_executor, functions = self.enter_bot_mode(functions_all)

            model_output = model_executor.call(self.chat_memory.get_history(), functions)

        return model_output
