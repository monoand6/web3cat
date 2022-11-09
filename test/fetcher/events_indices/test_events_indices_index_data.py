from web3cat.fetcher.events_indices.index_data import EventsIndexData


def test_events_index_data_to_dict():
    index = EventsIndexData()
    index.set_range(11000, 14000, True)
    assert index.to_dict() == {"startBlock": 8000, "endBlock": None, "mask": "0x38"}
