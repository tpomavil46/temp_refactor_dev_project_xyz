import pytest

from seeq.spy.workbooks import Analysis, ItemList, Topic


@pytest.mark.unit
def test_list():
    analysis = Analysis({
        'ID': '665B8740-F3AF-46DA-8022-255D8300720C',
        'Name': 'Analysis 1'
    })

    topic = Topic({
        'ID': '8FF50893-E81E-4A0E-9D4D-F18E59F89EF2',
        'Path': 'Folder 1',
        'Name': 'Topic 1'
    })

    workbooks = ItemList([analysis, topic])

    retrieved = workbooks['665B8740-F3AF-46DA-8022-255D8300720C']
    assert retrieved is analysis

    retrieved = workbooks['Analysis 1']
    assert retrieved is analysis

    retrieved = workbooks[0]
    assert retrieved is analysis

    retrieved = workbooks[-1]
    assert retrieved is topic

    retrieved = workbooks['8FF50893-E81E-4A0E-9D4D-F18E59F89EF2']
    assert retrieved is topic

    assert 'Topic 1' in workbooks

    retrieved = workbooks['Topic 1']
    assert retrieved is topic

    retrieved = workbooks['Folder 1 >> Topic 1']
    assert retrieved is topic

    retrieved = workbooks[1]
    assert retrieved is topic

    assert 'Analysis 2' not in workbooks

    with pytest.raises(IndexError):
        retrieved = workbooks['Analysis 2']

    with pytest.raises(IndexError):
        retrieved = workbooks[2]
