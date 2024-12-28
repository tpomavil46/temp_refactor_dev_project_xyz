from __future__ import annotations

import json
import os
import re
from typing import Optional, TYPE_CHECKING

from seeq.sdk import *
from seeq.spy._config import Setting

if TYPE_CHECKING:
    from seeq.spy._session import Session


def is_ipython():
    # noinspection PyBroadException
    try:
        from IPython import get_ipython
        return get_ipython() is not None
    except Exception:
        return False


# noinspection PyBroadException
def is_ipython_interactive():
    try:
        from IPython import get_ipython
        return get_ipython().__class__.__name__ == 'ZMQInteractiveShell'
    except Exception:
        return False


def is_jupyter():
    # noinspection PyBroadException
    try:
        return is_ipython_interactive() or is_rkernel()
    except Exception:
        return False


def is_rkernel():
    # noinspection PyBroadException
    try:
        return get_kernel_language() == 'R'
    except Exception:
        return False


def get_kernel_language():
    try:
        import psutil
    except ImportError:
        raise ImportError('`psutil` is required to get the kernel language. Please install it using '
                          f'`pip install seeq-spy[jupyter]` to use this feature.')
    return psutil.Process(os.getpid()).name()


def is_datalab():
    return os.environ.get('SEEQ_SDL_CONTAINER_IS_DATALAB') == 'true'


def is_datalab_addon_mode():
    return os.environ.get('SEEQ_DATALAB_ADDON_MODE') == 'true'


def is_datalab_api():
    return os.environ.get('SEEQ_DATALAB_API') == 'true'


def is_datalab_functions_project():
    return os.environ.get('SEEQ_PROJECT_TYPE', 'DATA_LAB') == "DATA_LAB_FUNCTIONS"


def is_executor():
    return os.environ.get('SEEQ_SDL_CONTAINER_IS_EXECUTOR') == 'true'


def get_label_from_executor():
    return os.environ.get('SEEQ_SDL_LABEL') or ''


def get_data_lab_project_name(session: Session, project_id=None) -> str:
    projects_api = ProjectsApi(session.client)
    if project_id is None:
        project_id = get_data_lab_project_id()
    return projects_api.get_project(id=project_id).name


def get_notebook_url(session: Session, use_private_url=True):
    project_url = get_data_lab_project_url(use_private_url)
    notebook = f"/notebooks/{get_notebook_path(session)}"

    return project_url + notebook


def get_notebook_path(session: Session):
    if is_datalab_api():
        raise RuntimeError('Cannot determine notebook path within Datalab API')

    try:
        import jupyter_client
    except ImportError:
        raise ImportError('`jupyter_client` is required to get the notebook path. Please install it using '
                          f'`pip install seeq-spy[jupyter]` to use this feature.')

    kernel_id = os.getenv('KERNEL_ID', re.search('kernel-(.*).json', jupyter_client.find_connection_file()).group(1))
    # noinspection PyBroadException
    try:
        response = session.requests.get(f'{get_data_lab_project_url(use_private_url=True)}/api/sessions')
        for nn in json.loads(response.text):
            if nn['kernel']['id'] == kernel_id:
                return nn['notebook']['path']
    except Exception:
        # CRAB-35351: If the API session JSON fails to load, fall back to the environment vars
        pass

    # We may be in Add On Mode where voila has its own kernel manager and api/sessions won't find the kernel
    # In that case, check the SCRIPT_NAME env variable which voila sets to '/data-lab/<projectId>/addon/<notebook_path>
    re_groups = re.search(f"/data-lab/{get_data_lab_project_id()}/(addon|apps)/(.*)", os.getenv('SCRIPT_NAME', ''))
    if re_groups is not None and len(re_groups.groups()) > 1:
        return re_groups.group(2)


def get_execution_notebook(lang: str) -> str:
    path = "/seeq/scheduling"
    if lang == "python":
        file = os.path.join(path, "ExecutionNotebook.ipynb")
    elif lang == "R":
        file = os.path.join(path, "ExecutionNotebookR.ipynb")
    else:
        raise FileNotFoundError(f"Could not find an execution notebook for language {lang}")

    return file


# noinspection PyBroadException
def get_notebook_language(nb_notebook) -> Optional[str]:
    try:
        from nbformat import NotebookNode
        return nb_notebook['metadata']['kernelspec']['language']
    except Exception:
        return None


def get_data_lab_orchestrator_url(use_private_url=True):
    return f'{Setting.get_private_url()}/data-lab' if use_private_url else f'{Setting.get_seeq_url()}/data-lab'


def get_data_lab_project_id():
    """
    Get Seeq ID assigned to this Data Lab Project

    Returns
    -------
    {str, None}
        The Seeq ID as a string, or None if no ID assigned
    """
    return Setting.SEEQ_PROJECT_UUID.get()


def get_data_lab_project_url(use_private_url=True):
    """
    Get Data Lab Project URL in form of ``{Seeq_Server_URL}/data-lab/{Data Lab Project ID}``

    Parameters
    ----------
    use_private_url : bool, default True
        If False, use the publicly accessible Seeq url for the Seeq_Server_URL

    Returns
    -------
    {str}
        The Data Lab Project URL as a string
    """
    return f'{get_data_lab_orchestrator_url(use_private_url)}/{get_data_lab_project_id()}'


def get_open_port() -> int:
    """
    Finds an open port on the host machine and returns it.

    Returns
    -------
    {int}
        An open port number

    Raises
    ------
    {RuntimeError}
        If no open ports are found
    """
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(('', 0))
            return s.getsockname()[1]
        except OSError:
            raise RuntimeError('Failed to find an open port.')
