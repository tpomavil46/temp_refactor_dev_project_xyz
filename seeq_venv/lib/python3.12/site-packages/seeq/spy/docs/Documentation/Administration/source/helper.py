import re
from seeq import spy
from seeq.sdk import *


def get_default_page_size():
    try:
        system_api = SystemApi(spy.client)
        system_settings = system_api.get_server_status(
            include_internal=True
        )  # type: ServerStatusOutputV1
        specs = system_settings.server_specs

        for spec in specs:
            if spec.component_name == "RecommendedPageSize":
                page_size = re.sub("[^0-9]", "", spec.system_spec_value)
                if page_size.isnumeric():
                    return int(page_size)
    except Exception as e:
        print("Seeq Server error encountered: %s" % e)

    # return a default value if we
    # could not get the page size
    return 10000
