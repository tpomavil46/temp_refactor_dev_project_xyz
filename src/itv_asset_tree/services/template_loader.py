# src/itv_asset_tree/services/template_loader.py
from importlib import import_module
from pathlib import Path
import inspect
from seeq.spy.assets import Asset

class TemplateLoader:
    def __init__(self, templates_path: str = "src/itv_asset_tree/templates"):
        self.templates_path = Path(templates_path)

    def load_templates(self):
        templates = []
        for file in self.templates_path.glob("*.py"):
            module_name = file.stem
            try:
                module = import_module(f"itv_asset_tree.templates.{module_name}")  # FIXED IMPORT PATH
                for name, obj in inspect.getmembers(module):
                    if inspect.isclass(obj) and issubclass(obj, Asset) and obj is not Asset:
                        templates.append({"name": name, "module": module_name})
            except ModuleNotFoundError as e:
                print(f"Error importing module {module_name}: {e}")  # Log the error instead of crashing
        return templates