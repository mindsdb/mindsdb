import yaml


def read_config():
    with open("config.yml", "rb") as f:
        return yaml.load(f, Loader=yaml.SafeLoader)


CONFIG = read_config()
