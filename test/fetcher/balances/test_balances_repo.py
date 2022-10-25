from hypothesis import given, settings, HealthCheck

from balances.strategies import balance
from fetcher.balances.repo import BalancesRepo
from fetcher.balances.balance import Balance


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(balance=balance())
def test_read_write(balance: Balance, balances_repo: BalancesRepo):
    c1 = balance
    c2 = Balance(
        chain_id=c1.chain_id,
        block_number=c1.block_number + 10,
        address=c1.address,
        balance=c1.balance,
    )
    c3 = Balance(
        chain_id=c1.chain_id,
        block_number=c1.block_number,
        address=c1.address + "312f0d98ff",
        balance=c1.balance,
    )
    balances_repo.save([c1, c2, c3])
    balances = list(balances_repo.find([c1.address], c1.block_number, c1.block_number))
    assert len(balances) == 0

    balances = list(
        balances_repo.find(
            [c1.address],
            c1.block_number,
            c1.block_number + 1,
        )
    )
    assert balances == [c1]

    balances = list(
        balances_repo.find(
            [c1.address],
            c1.block_number + 10,
            c1.block_number + 11,
        )
    )
    assert balances == [c2]

    balances = list(
        balances_repo.find(
            [c1.address, c3.address],
            c1.block_number,
            c1.block_number + 1,
        )
    )
    assert sorted(balances, key=lambda x: x.block_number) == sorted(
        [c1, c3], key=lambda x: x.block_number
    )

    balances = list(
        balances_repo.find(
            [c1.address],
            c1.block_number,
            c1.block_number + 11,
        )
    )
    assert sorted(balances, key=lambda x: x.block_number) == sorted(
        [c1, c2], key=lambda x: x.block_number
    )
    balances_repo.conn.rollback()
