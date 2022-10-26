from hypothesis import given
from fetcher.erc20_metas.erc20_meta import ERC20Meta
from erc20_metas.strategies import erc20_meta


@given(erc20_meta())
def test_erc20_meta_rows(meta: ERC20Meta):
    assert ERC20Meta.from_row(meta.to_row()) == meta


@given(erc20_meta())
def test_erc20_meta_dicts(meta: ERC20Meta):
    assert ERC20Meta.from_dict(meta.to_dict()) == meta
