from hypothesis import given, settings, HealthCheck

from events.strategies import event
from fetcher.events.repo import EventsRepo
from fetcher.events.event import Event


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(event=event())
def test_read_write(event: Event, events_repo: EventsRepo):
    e1 = event
    e2 = Event(
        chain_id=e1.chain_id,
        block_number=e1.block_number,
        transaction_hash=e1.transaction_hash,
        log_index=e1.log_index,
        address=e1.address,
        event=e1.event,
        args=e1.args,
    )
    e2.block_number += 10
    e2.log_index += 1
    events_repo.save([e1, e2])
    events = events_repo.find(e1.chain_id, e1.event, e1.address)
    assert sorted(events, key=lambda x: x.block_number) == sorted(
        [e1, e2], key=lambda x: x.block_number
    )
    events_repo.rollback()


def test_find(events_repo: EventsRepo):
    defaults = {
        "chain_id": 1,
        "block_number": 1,
        "transaction_hash": "0x1234",
        "address": "0xaddd",
        "event": "Transfer",
    }
    e1 = Event(
        args={"from": "0x1234", "to": "0x5678", "value": 10}, log_index=1, **defaults
    )
    e2 = Event(
        args={"from": "0x5678", "to": "0x1234", "value": 20}, log_index=2, **defaults
    )
    e3 = Event(
        args={"from": "0x1234", "to": "0x9090", "value": 30}, log_index=3, **defaults
    )
    e4 = Event(
        args={"from": "0x1010", "to": "0x2020", "value": 30}, log_index=4, **defaults
    )
    events_repo.save([e1, e2, e3, e4])
    assert list(
        events_repo.find(
            args_filters=None,
            chain_id=defaults["chain_id"],
            event="Transfer",
            address=defaults["address"],
        )
    ) == [e1, e2, e3, e4]
    assert list(
        events_repo.find(
            args_filters={},
            chain_id=defaults["chain_id"],
            event="Transfer",
            address=defaults["address"],
        )
    ) == [e1, e2, e3, e4]

    assert list(
        events_repo.find(
            args_filters={"from": "0x1234"},
            chain_id=defaults["chain_id"],
            event="Transfer",
            address=defaults["address"],
        )
    ) == [e1, e3]

    assert list(
        events_repo.find(
            args_filters={"to": "0x1234"},
            chain_id=defaults["chain_id"],
            event="Transfer",
            address=defaults["address"],
        )
    ) == [e2]

    assert list(
        events_repo.find(
            args_filters={"from": "0x5678"},
            chain_id=defaults["chain_id"],
            event="Transfer",
            address=defaults["address"],
        )
    ) == [e2]
    assert list(
        events_repo.find(
            args_filters={"from": ["0x1234", "0x5678"]},
            chain_id=defaults["chain_id"],
            event="Transfer",
            address=defaults["address"],
        )
    ) == [e1, e2, e3]

    assert list(
        events_repo.find(
            args_filters={"from": ["0x1234", "0x5678"], "to": "0x1234"},
            chain_id=defaults["chain_id"],
            event="Transfer",
            address=defaults["address"],
        )
    ) == [e2]

    assert list(
        events_repo.find(
            args_filters={"from": ["0x1234", "0x5678"], "to": ["0x1234", "0x9090"]},
            chain_id=defaults["chain_id"],
            event="Transfer",
            address=defaults["address"],
        )
    ) == [e2, e3]

    assert list(
        events_repo.find(
            args_filters={"value": 30},
            chain_id=defaults["chain_id"],
            event="Transfer",
            address=defaults["address"],
        )
    ) == [e3, e4]

    assert (
        list(
            events_repo.find(
                args_filters={"some": 30},
                chain_id=defaults["chain_id"],
                event="Transfer",
                address=defaults["address"],
            )
        )
        == []
    )

    assert (
        list(
            events_repo.find(
                args_filters={"some": 30, "from": "0x1234"},
                chain_id=defaults["chain_id"],
                event="Transfer",
                address=defaults["address"],
            )
        )
        == []
    )
