from configparser import ConfigParser


class TrinoConfigProvider:

    def __init__(self, **kwargs):
        self.config_file_name = kwargs.get('config_file_name')
        self.config_parser = ConfigParser()
        self.config_parser.read(self.config_file_name)

    def get_trino_kerberos_config(self):
        return self.config_parser['KERBEROS_CONFIG']
