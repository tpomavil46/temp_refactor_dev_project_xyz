import pytest

from seeq.spy.workbooks._data import Asset
from seeq.spy.workbooks._item import Item


@pytest.mark.unit
def test_asset_old_format():
    asset1 = Asset({'Name': 'Asset 1', 'Path': 'Path 1', 'Asset': 'Asset 1'})
    assert asset1.type == 'Asset'
    assert asset1.provenance == Item.CONSTRUCTOR
    assert asset1['Name'] == 'Asset 1'
    assert asset1['Asset'] == 'Asset 1'
    assert asset1['Path'] == 'Path 1'
    assert not asset1['Old Asset Format']

    asset2 = Item.load({'Type': 'Asset', 'Name': 'Asset 1', 'Path': 'Path 1', 'Asset': 'Asset 1'})
    assert asset2.type == 'Asset'
    assert asset2.provenance == Item.LOAD
    assert asset2['Name'] == 'Asset 1'
    assert asset2['Asset'] == 'Asset 1'
    # "Old Asset Format" wasn't included in the definition, so it's equivalent to old_asset_format=True
    assert asset2['Path'] == 'Path 1 >> Asset 1'
    assert not asset2['Old Asset Format']

    asset3 = Item.load({'Type': 'Asset', 'Name': 'Asset 1', 'Path': 'Path 1', 'Asset': 'Asset 1',
                        'Old Asset Format': False})
    assert asset3.type == 'Asset'
    assert asset3.provenance == Item.LOAD
    assert asset3['Name'] == 'Asset 1'
    assert asset3['Asset'] == 'Asset 1'
    assert asset3['Path'] == 'Path 1'
    assert not asset3['Old Asset Format']
