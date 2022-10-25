from hypothesis import given
from balances.strategies import balance
from fetcher.balances.balance import Balance


@given(balance())
def test_balance_tuples(balance: Balance):
    assert Balance.from_row(balance.to_row()) == balance


@given(balance())
def test_balance_to_from_dict(balance: Balance):
    assert Balance.from_dict(balance.to_dict()) == balance
