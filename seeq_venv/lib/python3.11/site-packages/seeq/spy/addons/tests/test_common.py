from seeq.sdk import *
from seeq.spy import Session


def enable_addon_tools(session: Session, value):
    system_api = SystemApi(session.client)
    config_option_input = ConfigurationOptionInputV1(path='Features/AddOnTools/Enabled', value=value)
    system_api.set_configuration_options(body=ConfigurationInputV1([config_option_input]))
