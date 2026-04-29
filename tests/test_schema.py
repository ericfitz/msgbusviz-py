from msgbusviz._schema import validate_message


def test_valid_send_message():
    ok, _ = validate_message({"type": "sendMessage", "channel": "x"})
    assert ok


def test_invalid_color():
    ok, _ = validate_message({"type": "sendMessage", "channel": "x", "color": "lime"})
    assert not ok


def test_unknown_type():
    ok, _ = validate_message({"type": "banana"})
    assert not ok
