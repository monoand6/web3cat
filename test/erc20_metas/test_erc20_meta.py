from hypothesis import given
from events.strategies import event
from fetcher.erc20_metas.erc20_meta import ERC20Meta
from fetcher.events.event import Event
from erc20_metas.strategies import erc20_meta


@given(erc20_meta())
def test_tuples(event: ERC20Meta):
    assert ERC20Meta.from_tuple(event.to_tuple()) == event
