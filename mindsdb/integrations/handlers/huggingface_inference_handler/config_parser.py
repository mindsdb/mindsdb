import configparser
import yaml


class ConfigParser(configparser.ConfigParser):
    def read_yaml(self, filepath):
        with open(filepath, 'r') as f:
            yaml_config = yaml.load(f, Loader=yaml.FullLoader)
            for section, options in yaml_config.items():
                self.add_section(section)
                for option, value in options.items():
                    self.set(section, option, str(value))