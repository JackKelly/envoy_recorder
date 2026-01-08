import json
import pytest
from envoy_recorder.envoy_recorder import WrappedEnvoyData


def test_wrapped_envoy_data_to_json() -> None:
    retrieval_time = 1609459200
    envoy_json = '{"production": [{"type": "inverters", "activeCount": 29}]}'
    data = WrappedEnvoyData(retrieval_time=retrieval_time, envoy_json=envoy_json)

    json_string = data.to_json()

    # Verify it is valid JSON
    parsed = json.loads(json_string)

    assert parsed["retrieval_time"] == retrieval_time
    assert parsed["envoy_json"] == json.loads(envoy_json)


def test_wrapped_envoy_data_to_json_with_simple_value() -> None:
    retrieval_time = 123456789
    envoy_json = '{"foo": "bar"}'
    data = WrappedEnvoyData(retrieval_time=retrieval_time, envoy_json=envoy_json)

    json_string = data.to_json()
    parsed = json.loads(json_string)

    assert parsed["retrieval_time"] == retrieval_time
    assert parsed["envoy_json"] == {"foo": "bar"}


def test_wrapped_envoy_data_to_json_with_invalid_inner_json() -> None:
    # If envoy_json is not valid JSON, to_json currently produces invalid JSON
    # because it just splices the string in.
    retrieval_time = 123
    envoy_json = "not json"
    data = WrappedEnvoyData(retrieval_time=retrieval_time, envoy_json=envoy_json)

    json_string = data.to_json()
    assert json_string == '{"retrieval_time": 123, "envoy_json": not json}'

    with pytest.raises(json.JSONDecodeError):
        json.loads(json_string)
