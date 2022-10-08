from hypothesis import given
from calls.strategies import call
from fetcher.calls.call import Call


@given(call())
def test_call_tuples(call: Call):
    assert Call.from_tuple(call.to_tuple()) == call
