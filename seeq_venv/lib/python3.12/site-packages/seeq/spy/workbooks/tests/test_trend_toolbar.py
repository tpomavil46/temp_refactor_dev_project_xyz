import pytest
from unittest.mock import MagicMock

from seeq.spy.workbooks._trend_toolbar import ContextSwitchable, TrendToolbar, Labels, Signals, Conditions, Cursors
from seeq.spy.workbooks import AnalysisWorksheet, AnalysisWorkstep, Analysis
from seeq.spy._errors import *


@pytest.fixture
def worksheet():
    workbook = Analysis()
    worksheet = AnalysisWorksheet(workbook)
    return worksheet


@pytest.fixture
def workstep():
    workstep = AnalysisWorkstep()
    return workstep


@pytest.mark.unit
def test_context_switchable(worksheet, workstep):
    """Test the ContextSwitchable class."""
    context = ContextSwitchable(worksheet)
    assert context._context == "worksheet"
    context = ContextSwitchable(workstep)
    assert context._context == "workstep"

    with pytest.raises(ValueError, match="Unsupported parent class"):
        ContextSwitchable(MagicMock())


def test_trend_toolbar_from_worksheet_and_workstep(worksheet, workstep):
    """Test the trend_toolbar property from a worksheet."""
    assert isinstance(worksheet.trend_toolbar, TrendToolbar)
    assert isinstance(workstep.trend_toolbar, TrendToolbar)

    assert isinstance(worksheet.trend_toolbar._getter_workstep, AnalysisWorkstep)
    assert isinstance(workstep.trend_toolbar._getter_workstep, AnalysisWorkstep)
    assert isinstance(worksheet.trend_toolbar._setter_workstep, AnalysisWorkstep)
    assert isinstance(workstep.trend_toolbar._setter_workstep, AnalysisWorkstep)

    assert workstep.trend_toolbar._getter_workstep == workstep
    assert workstep.trend_toolbar._setter_workstep == workstep

    worksheet.current_workstep = MagicMock()
    current_workstep = worksheet.trend_toolbar._getter_workstep
    assert isinstance(current_workstep, MagicMock)
    worksheet.current_workstep.assert_called_once()

    worksheet.branch_current_workstep = MagicMock()
    branch_current_workstep = worksheet.trend_toolbar._setter_workstep
    assert isinstance(branch_current_workstep, MagicMock)
    worksheet.branch_current_workstep.assert_called_once()


@pytest.mark.unit
def test_trend_toolbar_view(worksheet, workstep):
    """Test the view property of TrendToolbar."""
    toolbar = TrendToolbar(worksheet)
    assert toolbar.view == worksheet.view
    toolbar.view = "Scatter Plot"
    assert worksheet.view == "Scatter Plot"

    toolbar = TrendToolbar(workstep)
    assert toolbar.view == workstep.view
    toolbar.view = "Scatter Plot"
    assert workstep.view == "Scatter Plot"


@pytest.mark.unit
def test_trend_toolbar_show_grid_lines(worksheet, workstep):
    """Test show_grid_lines property."""
    toolbar = TrendToolbar(worksheet)

    # Test default value
    assert toolbar.show_grid_lines is False
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["showGridlines"] is False

    # Test setter
    toolbar.show_grid_lines = True
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["showGridlines"] is True

    toolbar = TrendToolbar(workstep)

    # Test default value
    assert toolbar.show_grid_lines is False
    assert workstep.get_workstep_stores()["sqTrendStore"]["showGridlines"] is False

    # Test setter
    toolbar.show_grid_lines = True
    assert toolbar.show_grid_lines is True
    assert workstep.get_workstep_stores()["sqTrendStore"]["showGridlines"] is True

    with pytest.raises(SPyTypeError, match="'show_grid_lines' must be a boolean"):
        toolbar.show_grid_lines = "dummy"


@pytest.mark.unit
def test_trend_toolbar_hide_uncertainty(worksheet, workstep):
    """Test hide_uncertainty property."""
    toolbar = TrendToolbar(worksheet)

    # Test default value
    assert toolbar.hide_uncertainty is False
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["hideUncertainty"] is False

    # Test setter
    toolbar.hide_uncertainty = True
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["hideUncertainty"] is True

    toolbar = TrendToolbar(workstep)

    # Test default value
    assert toolbar.hide_uncertainty is False
    assert workstep.get_workstep_stores()["sqTrendStore"]["hideUncertainty"] is False

    # Test setter
    toolbar.hide_uncertainty = True
    assert toolbar.hide_uncertainty is True
    assert workstep.get_workstep_stores()["sqTrendStore"]["hideUncertainty"] is True

    with pytest.raises(SPyTypeError, match="'hide_uncertainty' must be a boolean"):
        toolbar.hide_uncertainty = "dummy"


@pytest.mark.unit
def test_trend_toolbar_dimming(worksheet, workstep):
    """Test dimming property."""
    toolbar = TrendToolbar(worksheet)

    # Test default value
    assert toolbar.dimming is False
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["hideUnselectedItems"] is False

    # Test setter
    toolbar.dimming = True
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["hideUnselectedItems"] is True

    toolbar = TrendToolbar(workstep)

    # Test default value
    assert toolbar.dimming is False
    assert workstep.get_workstep_stores()["sqTrendStore"]["hideUnselectedItems"] is False

    # Test setter
    toolbar.dimming = True
    assert toolbar.dimming is True
    assert workstep.get_workstep_stores()["sqTrendStore"]["hideUnselectedItems"] is True

    with pytest.raises(SPyTypeError, match="'dimming' must be a boolean"):
        toolbar.dimming = "dummy"


@pytest.mark.unit
def test_labels(worksheet, workstep):
    """Test the Labels class."""
    toolbar = TrendToolbar(worksheet)
    labels = toolbar.labels

    # Test signals, conditions, and cursors
    assert isinstance(labels, Labels)
    assert isinstance(labels.signals, Signals)
    assert isinstance(labels.conditions, Conditions)
    assert isinstance(labels.cursors, Cursors)


@pytest.mark.unit
def test_labels_signals(worksheet, workstep):
    """Test the Signals class."""

    toolbar = TrendToolbar(worksheet)
    signals = toolbar.labels.signals

    # Test default name label Display Configuration
    assert signals.name == "lane"
    signals.name = "off"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "name"] == "off"
    signals.name = "axis"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "name"] == "axis"

    # Test invalid name label Display Configuration
    with pytest.raises(SPyValueError, match="'name' must be one of"):
        signals.name = "invalid"

    # Test default description label Display Configuration
    assert signals.description == "off"
    signals.description = "lane"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "description"] == "lane"
    signals.description = "axis"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "description"] == "axis"

    # Test invalid description label Display Configuration
    with pytest.raises(SPyValueError, match="'description' must be one of"):
        signals.description = "invalid"

    # Test default asset Display Configuration
    assert signals.asset == "off"
    signals.asset = "lane"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "asset"] == "lane"
    signals.asset = "axis"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "asset"] == "axis"

    # Test invalid description label Display Configuration
    with pytest.raises(SPyValueError, match="'asset' must be one of"):
        signals.asset = "invalid"

    # Test default asset assetPathLevels Display Configuration
    assert signals.asset_path_levels == 1
    signals.asset_path_levels = 3
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "assetPathLevels"] == 3

    # Test invalid assetPathLevels label Display Configuration
    with pytest.raises(SPyValueError, match="'asset_path_levels' must be an Integer"):
        signals.asset_path_levels = "invalid"

    # Test default line_style Display Configuration
    assert signals.line_style == "off"
    signals.line_style = "lane"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "line"] == "lane"
    signals.line_style = "axis"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "line"] == "axis"

    # Test invalid line_style label Display Configuration
    with pytest.raises(SPyValueError, match="'line_style' must be one of"):
        signals.line_style = "invalid"

    # Test default unit_of_measure Display Configuration
    assert signals.unit_of_measure == "axis"
    signals.unit_of_measure = "lane"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "unitOfMeasure"] == "lane"
    signals.unit_of_measure = "off"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "unitOfMeasure"] == "off"

    # Test invalid unit_of_measure label Display Configuration
    with pytest.raises(SPyValueError, match="'unit_of_measure' must be one of"):
        signals.unit_of_measure = "invalid"

    # Test default custom Display Configuration
    assert signals.custom == "off"
    signals.custom = "lane"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "custom"] == "lane"
    signals.custom = "axis"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "custom"] == "axis"

    # Test invalid custom label Display Configuration
    with pytest.raises(SPyValueError, match="'custom' must be one of"):
        signals.custom = "invalid"

    # Test default custom_labels Display Configuration
    assert signals.custom_labels == []

    signals.custom = "off"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "customLabels"] == []
    # custom_labels is not set when custom is off
    signals.custom_labels = ["some_label"]
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "customLabels"] == []

    custom_labels = ["custom label 1", "custom label 2", "custom label 3"]
    expected_lane_custom_labels = [{'location': 'lane', 'target': 1, 'text': 'custom label 1'},
                                   {'location': 'lane', 'target': 2, 'text': 'custom label 2'},
                                   {'location': 'lane', 'target': 3, 'text': 'custom label 3'}]
    expected_axis_custom_labels = [{'location': 'axis', 'target': 'A', 'text': 'custom label 1'},
                                   {'location': 'axis', 'target': 'B', 'text': 'custom label 2'},
                                   {'location': 'axis', 'target': 'C', 'text': 'custom label 3'}]

    signals.custom = "lane"
    signals.custom_labels = custom_labels
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "customLabels"] == expected_lane_custom_labels

    signals.custom = "axis"
    # when custom is changed, the custom_labels is reset
    assert signals.custom_labels == []

    signals.custom_labels = custom_labels
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["labelDisplayConfiguration"][
               "customLabels"] == expected_axis_custom_labels

    with pytest.raises(SPyTypeError, match="'custom_labels' must be a list of strings"):
        signals.custom_labels = "invalid"


@pytest.mark.unit
def test_labels_conditions(worksheet, workstep):
    """Test the Conditions class."""

    toolbar = TrendToolbar(worksheet)
    conditions = toolbar.labels.conditions

    # Test default name label Display Configuration
    assert conditions.name == "lane"
    conditions.name = "off"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["showCapsuleLaneLabels"] is False
    conditions.name = "lane"
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["showCapsuleLaneLabels"] is True

    with pytest.raises(SPyValueError, match="'name' must be either 'off' or 'lane'"):
        conditions.name = "invalid"

    assert conditions.capsules == []
    capsules = ["capsule1", "capsule2"]
    expected_capsules = {'capsule1': True, 'capsule2': True}

    conditions.capsules = capsules
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["enabledColumns"][
               "CAPSULES"] == expected_capsules
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["enabledColumns"][
               "CHART_CAPSULES"] == expected_capsules

    capsules_properties = ["capsule1", "properties.capsule2"]
    expected_capsules = {'capsule1': True, 'properties.capsule2': True}
    expected_capsules_properties = {
        'properties.capsule2': {'key': 'properties.capsule2',
                                'propertyName': 'capsule2',
                                'style': 'string',
                                'uomKey': 'propertiesUOM.capsule2'}
    }
    conditions.capsules = capsules_properties
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["propertyColumns"][
               "CAPSULES"] == expected_capsules_properties
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["propertyColumns"][
               "CHART_CAPSULES"] == expected_capsules_properties

    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["enabledColumns"][
               "CAPSULES"] == expected_capsules
    assert worksheet.current_workstep().get_workstep_stores()["sqTrendStore"]["enabledColumns"][
               "CHART_CAPSULES"] == expected_capsules

    with pytest.raises(SPyTypeError, match="'capsules' must be a list of strings"):
        conditions.capsules = "invalid"


@pytest.mark.unit
def test_labels_cursors(worksheet, workstep):
    """Test the Cursors class."""

    toolbar = TrendToolbar(worksheet)
    cursors = toolbar.labels.cursors

    # Test default values label Display Configuration
    assert cursors.values == "show"

    cursors.values = "hide"
    assert worksheet.current_workstep().get_workstep_stores()["sqCursorStore"]["showValues"] is False

    cursors.values = "show"
    assert worksheet.current_workstep().get_workstep_stores()["sqCursorStore"]["showValues"] is True

    with pytest.raises(SPyValueError, match="'values' must be either 'show' or 'hide'"):
        cursors.values = "invalid"
