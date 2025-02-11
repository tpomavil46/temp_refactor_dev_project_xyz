from __future__ import annotations

import configparser
import os
from enum import Enum

from seeq.base import util

file_config = None


class Setting(Enum):
    CONFIG_FOLDER = {'env': 'SEEQ_SPY_CONFIG_FOLDER', 'ini': None}
    CONFIG_FILENAME = {'env': 'SEEQ_SPY_CONFIG_FILENAME', 'ini': None}
    SEEQ_URL = {'env': 'SEEQ_SERVER_URL', 'ini': 'seeq_server_url'}
    PRIVATE_URL = {'env': 'SEEQ_PRIVATE_URL', 'ini': None}
    SEEQ_CERT_PATH = {'env': 'SEEQ_CERT_PATH', 'ini': 'seeq_cert_path'}
    SEEQ_KEY_PATH = {'env': 'SEEQ_KEY_PATH', 'ini': 'seeq_key_path'}
    AGENT_KEY_PATH = {'env': 'AGENT_KEY_PATH', 'ini': 'agent_key_path'}
    SEEQ_PROJECT_UUID = {'env': 'SEEQ_PROJECT_UUID', 'ini': None}

    def get_env_name(self):
        return self.value['env']

    def get_ini_name(self):
        return self.value['ini']

    def get(self):
        setting = os.environ.get(self.get_env_name())
        if not setting and self.get_ini_name():
            # noinspection PyBroadException
            try:
                config = Setting.get_file_config()
                setting = config.get('spy', self.get_ini_name(), fallback=None)
            except Exception:
                # This can happen on a machine where the home folder is not accessible, like on Spark / AWS Glue
                return None

        return setting

    def set(self, value):
        os.environ[self.get_env_name()] = value

    def unset(self):
        del os.environ[self.get_env_name()]

    @staticmethod
    def get_config_folder():
        """
        This is the config folder for the SPy library, which is where any additional configuration files for SPy must be
        stored. The default location is the same as the Seeq global folder.
        :return: Location of the config folder
        """
        config_folder = Setting.CONFIG_FOLDER.get()
        if not config_folder:
            if util.is_windows():
                config_folder = os.path.join(os.environ["ProgramData"], 'Seeq')
            else:
                config_folder = os.path.join(util.get_home_dir(), '.seeq')

        util.safe_makedirs(config_folder, exist_ok=True)

        return config_folder

    @staticmethod
    def set_config_folder(path):
        Setting.CONFIG_FOLDER.set(path)

    @staticmethod
    def get_config_filename():
        filename = Setting.CONFIG_FILENAME.get()
        return filename if filename else "spy.ini"

    @staticmethod
    def get_config_path():
        return os.path.join(Setting.get_config_folder(), Setting.get_config_filename())

    @staticmethod
    def get_seeq_url():
        url = Setting.SEEQ_URL.get()
        return url if url else 'http://localhost:34216'

    @staticmethod
    def set_seeq_url(url):
        Setting.SEEQ_URL.set(url)

    @staticmethod
    def unset_seeq_url():
        if Setting.SEEQ_URL.get() is not None:
            Setting.SEEQ_URL.unset()

    @staticmethod
    def get_private_url():
        url = Setting.PRIVATE_URL.get()
        return url if url else Setting.get_seeq_url()

    @staticmethod
    def set_private_url(url):
        Setting.PRIVATE_URL.set(url)

    @staticmethod
    def unset_private_url():
        if Setting.PRIVATE_URL.get() is not None:
            Setting.PRIVATE_URL.unset()

    @staticmethod
    def get_seeq_cert_path():
        path = Setting.SEEQ_CERT_PATH.get()
        if path:
            return path
        else:
            # noinspection PyBroadException
            try:
                return os.path.join(Setting.get_config_folder(), 'keys', 'seeq-cert.pem')
            except Exception:
                # This can happen on a machine where the home folder is not accessible, like on Spark / AWS Glue
                return None

    @staticmethod
    def get_seeq_key_path():
        path = Setting.SEEQ_KEY_PATH.get()
        if path:
            return path
        else:
            # noinspection PyBroadException
            try:
                return os.path.join(Setting.get_config_folder(), 'keys', 'seeq-key.pem')
            except Exception:
                # This can happen on a machine where the home folder is not accessible, like on Spark / AWS Glue
                return None

    @staticmethod
    def get_file_config():
        global file_config
        if not file_config:
            file_config = configparser.ConfigParser()
            file_config.read(Setting.get_config_path())
        return file_config


# For compatibility with older versions of Data Lab
def set_seeq_url(url):
    Setting.SEEQ_URL.set(url)
