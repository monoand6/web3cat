from events.strategies import event
from probe.events.repo import EventsRepo
from probe.events.event import Event


def test_read_write(events_repo: EventsRepo):
    e1 = event().example()
    e2 = Event(**e1.__dict__)
    e2.block_number += 10
    e2.log_index += 1
    events_repo.save([e1, e2])
    events = events_repo.find(e1.event, e1.address)
    assert sorted(events, key=lambda x: x.block_number) == sorted(
        [e1, e2], key=lambda x: x.block_number
    )
