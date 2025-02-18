import pytest
from itv_asset_tree.managers.template_manager import AcceleratorTemplateManager
from itv_asset_tree.utilities.template_loader import TemplateLoader
from itv_asset_tree.utilities.template_parameters import TemplateParameters
import os
import json

def test_template_loader_load_template():
    loader = TemplateLoader(template_directory='schemas')
    template_data = loader.load_template('hvac_template')
    assert isinstance(template_data, dict)

def test_template_loader_list_templates():
    loader = TemplateLoader(template_directory='schemas')
    templates = loader.list_available_templates()
    assert isinstance(templates, list)

def test_template_parameters_validation():
    parameters = TemplateParameters(template_type='hvac_template', parameters={'param1': 'value1'})
    with pytest.raises(Exception):
        parameters.validate_parameters()

def test_accelerator_template_manager_load_and_apply(mocker):
    mock_spy = mocker.patch('seeq.spy')
    manager = AcceleratorTemplateManager(template_type='hvac_template', parameters={'param1': 'value1'})
    template = manager.load_template()
    assert template is not None
    configured_template = manager.configure_template(template)
    assert configured_template is not None
    manager.apply_template(configured_template, 'Test_Asset_Tree')
