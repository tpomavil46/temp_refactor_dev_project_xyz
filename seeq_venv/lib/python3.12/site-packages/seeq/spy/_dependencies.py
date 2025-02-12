from typing import Optional, List


# Class for resolving dependencies based on user input or installed packages
class Dependencies:
    # Mapping of dependencies to each extra
    # This should remain updated with the extras_require in seeq-spy/setup.sq-variables.py
    dependency_mapping = {
        'all': ['bs4', 'chevron', 'cron_descriptor', 'ipylab', 'IPython', 'ipywidgets', 'jupyterlab', 'mako',
                'markdown', 'matplotlib', 'nbconvert', 'nbformat', 'notebook', 'psutil', 'recurrent', 'setuptools'],
        'jobs': ['cron_descriptor', 'nbconvert', 'nbformat', 'recurrent'],
        'jupyter': ['ipylab', 'IPython', 'ipywidgets', 'jupyterlab', 'nbconvert', 'nbformat', 'notebook', 'psutil',
                    'setuptools'],
        'templates': ['bs4', 'chevron', 'mako', 'markdown', 'matplotlib'],
        'widgets': ['IPython', 'ipywidgets', 'matplotlib', 'seeq-data-lab-env-mgr']
    }

    def __init__(self, user_extras: Optional[List[str]] = None):
        """
        Initializes the installed extras based on the user's input. If no input is provided, the installed packages
        will be scanned, and existing extras will be saved to installed_extras.
        """
        self.installed_extras = []

        if isinstance(user_extras, list):
            for user_extra in user_extras:
                if user_extra == 'all':
                    self.installed_extras = ['all']
                    break
                if user_extra in self.dependency_mapping.keys():
                    self.installed_extras.append(user_extra)
        else:
            imported_packages = set()
            for extra in list(self.dependency_mapping.keys()):
                dependencies = self.dependency_mapping.get(extra, [])
                missing_dependencies = False

                for dependency in dependencies:
                    if dependency in imported_packages:
                        continue
                    try:
                        __import__(dependency)
                    except ImportError:
                        missing_dependencies = True
                        break
                    imported_packages.add(dependency)
                if missing_dependencies:
                    continue

                self.installed_extras.append(extra)
                if extra == 'all':
                    break

    def __str__(self):
        """
        Returns the installed extras as a comma separated string enclosed in square brackets.
        :return:
        """
        return '[' + ','.join(self.installed_extras) + ']' if self.installed_extras else ''
