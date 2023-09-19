from walrus import Database

class ML_TASK_TYPE:
    LEARN = 1
    FINETUNE = 2
    PREDICT = 3

TASKS_STREAM_NAME = 'ml-tasks'

class MLTaskProducer:
    def __init__(self) -> None:
        db = Database()
        self.stream = db.Stream(TASKS_STREAM_NAME)

    def add_async(self, args, kwargs) -> str:
        '''
            Returns:
                str: task key in queue
        '''
        msgid = self.stream.add({'message': 'hello streams'})
        pass

    def add(self, args, kwargs) -> object:
        pass
        # yeld [status]
        # yeld [status, result]

    def get_task_status():
        pass


class MLTaskConsumer:
    def __init__(self) -> None:
        pass

    def run(self):
        # connect
        db = Database()
        self.stream = db.Stream(TASKS_STREAM_NAME)
        consumer_group = db.consumer_group('ml_executors', [TASKS_STREAM_NAME])
        # x = self.stream.consumers_info(consumer_group)
        message = consumer_group.read(count=1, block=1000, consumer='ml_executor')
        x = 1
        # attach to consumer group
        # go to wait cycle
        pass



ml_task_queue = MLTaskProducer()
