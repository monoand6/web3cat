from hypothesis import given
from calls.strategies import call
from fetcher.calls.call import Call


@given(call())
def test_call_tuples(call: Call):
    assert Call.from_tuple(call.to_tuple()) == call


@given(call())
def test_call_dicts(call: Call):
    assert Call.from_dict(call.to_dict()) == call