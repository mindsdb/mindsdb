from mindsdb.utilities.config import Config
from mindsdb.api.mongo.server import run_server


def start(config, initial=False):
    if not initial:
        print('\n\nWarning, this process should not have been started... nothing is "wrong" but it needlessly ate away a tiny bit of precious compute !\n\n')
    config = Config(config)
    run_server(config)


if __name__ == '__main__':
    start()
