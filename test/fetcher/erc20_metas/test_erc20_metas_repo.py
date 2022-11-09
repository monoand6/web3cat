from hypothesis import given, settings, HealthCheck

from erc20_metas.strategies import erc20_meta
from web3cat.fetcher.erc20_metas.repo import ERC20MetasRepo
from web3cat.fetcher.events.repo import EventsRepo
from web3cat.fetcher.events.event import Event
from web3cat.fetcher.erc20_metas.erc20_meta import ERC20Meta


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(erc20_meta=erc20_meta())
def test_read_write(erc20_meta: ERC20Meta, erc20_metas_repo: ERC20MetasRepo):
    erc20_metas_repo.save([erc20_meta])
    e2 = erc20_metas_repo.find(erc20_meta.symbol)
    e3 = erc20_metas_repo.find(erc20_meta.address)
    assert e2
    assert erc20_meta == e2
    assert e2 == e3
    erc20_metas_repo.conn.rollback()
