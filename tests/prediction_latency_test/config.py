import yaml


def read_config():
    with open("config.yml", "rb") as f:
        # return yaml.load(f, Loader=yaml.CLoader)
        return yaml.load(f)


CONFIG = read_config()
