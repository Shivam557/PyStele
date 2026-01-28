import os
import tempfile
import pytest

from checkpoint.save import save_checkpoint
from checkpoint.restore import restore_checkpoint
from checkpoint.exceptions import UnserializableError, ChecksumMismatchError


def test_save_restore_primitives():
    x = 123
    y = {"a": 1, "b": [1, 2, 3]}
    z = "hello"

    with tempfile.TemporaryDirectory() as d:
        ckpt = save_checkpoint("exp1", locals(), d)
        ns = {}
        restore_checkpoint(os.path.join(d, ckpt), ns)

        assert ns["x"] == x
        assert ns["y"] == y
        assert ns["z"] == z


def test_unserializable_function():
    def f():
        pass

    with tempfile.TemporaryDirectory() as d:
        with pytest.raises(UnserializableError):
            save_checkpoint("exp2", {"f": f}, d)


def test_checksum_tamper():
    x = 5
    with tempfile.TemporaryDirectory() as d:
        ckpt = save_checkpoint("exp3", locals(), d)
        obj_path = os.path.join(d, ckpt, "objects.bin")

        with open(obj_path, "ab") as f:
            f.write(b"corrupt")

        with pytest.raises(ChecksumMismatchError):
            restore_checkpoint(os.path.join(d, ckpt), {})
