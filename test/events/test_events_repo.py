from hypothesis import given, settings, HealthCheck

from events.strategies import event
from probe.events.repo import EventsRepo
from probe.events.event import Event


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
