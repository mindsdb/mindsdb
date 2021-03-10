# @TODO, replace with arrow later: https://mirai-solutions.ch/news/2020/06/11/apache-arrow-flight-tutorial/
import xmlrpc.client

class ModelInterface():
    def __init__(self):
        self.config = Config()

    def analyse_dataset(self, ds):
        return F.analyse_dataset(ds)
