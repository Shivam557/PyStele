from pystele.core.hashing import content_hash


def test_determinism():
    obj = {"b": [1, 2], "a": True}
    assert content_hash(obj) == content_hash(obj)


def test_small_difference_changes_hash():
    assert content_hash({"a": 1}) != content_hash({"a": 2})
