import configparser
import yaml


class ConfigParser():
    def __init__(self, file_path):
        with open(file_path, 'r') as f:
            self.config_dict = yaml.safe_load(f)

    def get_config_dict(self):
        return self.config_dict