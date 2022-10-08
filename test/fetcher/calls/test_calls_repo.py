from hypothesis import given, settings, HealthCheck

from calls.strategies import call
from fetcher.calls.repo import CallsRepo
from fetcher.calls.call import Call


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(call=call())
def test_read_write(call: Call, calls_repo: CallsRepo):
    c1 = call
    c2 = Call(
        chain_id=c1.chain_id,
        block_number=c1.block_number,
        address=c1.address,
        calldata=c1.calldata,
        response=c1.response,
    )
    c2.block_number += 10
    calls_repo.save([c1, c2])
    calls = calls_repo.find(
        c1.chain_id, c1.address, c1.calldata, c1.block_number, c1.block_number
    )
    assert len(calls) == 0
    calls = calls_repo.find(
        c1.chain_id, c1.address, c1.calldata, c1.block_number, c1.block_number + 1
    )
    assert calls == [c1]
    calls = calls_repo.find(
        c1.chain_id, c1.address, c1.calldata, c1.block_number + 10, c1.block_number + 11
    )
    assert calls == [c2]
    calls = calls_repo.find(
        c1.chain_id, c1.address, c1.calldata, c1.block_number, c1.block_number + 11
    )

    assert sorted(calls, key=lambda x: x.block_number) == sorted(
        [c1, c2], key=lambda x: x.block_number
    )
    calls_repo.rollback()
