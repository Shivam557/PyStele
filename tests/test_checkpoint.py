import os
import json
import tempfile
import hashlib
import pytest


from checkpoint.save import save_checkpoint
from checkpoint.restore import restore_checkpoint
from checkpoint.exceptions import (
    UnserializableError,
    ChecksumMismatchError,
    AtomicWriteError,
)


# -------------------------
# Test 1: basic save/restore
# -------------------------
def test_day2_save_restore_basic():
    def work():
        x = 10
        y = {"a": [1, 2, 3]}
        return save_checkpoint("exp1", locals(), tmpdir)

    with tempfile.TemporaryDirectory() as tmpdir:
        ckpt_id = work()
        ns = {}

        restore_checkpoint(os.path.join(tmpdir, ckpt_id), ns)

        assert ns["x"] == 10
        assert ns["y"] == {"a": [1, 2, 3]}


# -------------------------
# Test 2: deterministic ID
# -------------------------
def test_checkpoint_determinism():
    with tempfile.TemporaryDirectory() as tmpdir:
        data = {"x": 1, "y": [2, 3]}

        id1 = save_checkpoint("exp", data, tmpdir)
        id2 = save_checkpoint("exp", data, tmpdir)

        assert id1 == id2


# -------------------------
# Test 3: per-object corruption
# -------------------------
def test_per_object_checksum_failure():
    with tempfile.TemporaryDirectory() as tmpdir:
        data = {"x": 123, "y": 456}
        ckpt_id = save_checkpoint("exp", data, tmpdir)

        ckpt_path = os.path.join(tmpdir, ckpt_id)

        # Load objects.idx to find offset of "y"
        with open(os.path.join(ckpt_path, "objects.idx")) as f:
            idx = json.load(f)

        entry = idx["y"]
        offset = entry["offset"]

        # Corrupt a single byte in objects.bin
        obj_path = os.path.join(ckpt_path, "objects.bin")
        with open(obj_path, "r+b") as f:
            f.seek(offset)
            f.write(b"\x00")

        with pytest.raises(ChecksumMismatchError):
            restore_checkpoint(ckpt_path, {})


# -------------------------
# Test 4: atomicity on failure
# -------------------------
def test_atomic_write_failure(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:

        def fail_replace(src, dst):
            raise RuntimeError("simulated crash")

        monkeypatch.setattr(os, "replace", fail_replace)

        with pytest.raises(AtomicWriteError):
            save_checkpoint("exp", {"x": 1}, tmpdir)

        # Ensure no final checkpoint directory exists
        remaining = os.listdir(tmpdir)
        assert all(not name.startswith("a") for name in remaining)



from checkpoint.save import save_checkpoint
from checkpoint.restore import restore_checkpoint
from checkpoint.exceptions import UnserializableError

# Optional NumPy
try:
    import numpy as np
except ImportError:
    np = None


def test_numpy_array_save_restore():
    if np is None:
        pytest.skip("NumPy not installed")

    with tempfile.TemporaryDirectory() as tmpdir:
        def work():
            a = np.array([[1, 2], [3, 4]], dtype=np.float32)
            return save_checkpoint("exp-np", locals(), tmpdir)

        ckpt_id = work()
        ns = {}
        restore_checkpoint(os.path.join(tmpdir, ckpt_id), ns)

        assert "a" in ns
        assert np.array_equal(ns["a"], np.array([[1, 2], [3, 4]], dtype=np.float32))
        assert ns["a"].dtype == np.float32


def test_reject_open_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        f = open(__file__, "r")
        try:
            with pytest.raises(UnserializableError):
                save_checkpoint("exp-bad", {"f": f}, tmpdir)
        finally:
            f.close()


def test_reject_generator():
    with tempfile.TemporaryDirectory() as tmpdir:
        gen = (i for i in range(3))
        with pytest.raises(UnserializableError):
            save_checkpoint("exp-gen", {"g": gen}, tmpdir)


def test_metadata_contains_trace_info(tmp_path):
    from checkpoint.save import save_checkpoint

    def work():
        x = 1
        return save_checkpoint(
            "exp-meta",
            locals(),
            tmp_path,
            include=["x"]
        )


    ckpt_id = work()

    meta_path = tmp_path / ckpt_id / "metadata.json"
    assert meta_path.exists()

    import json
    meta = json.loads(meta_path.read_text())

    assert "caller" in meta
    assert "file" in meta["caller"]
    assert "function" in meta["caller"]
    assert "line" in meta["caller"]

    assert "environment" in meta
    assert "python_version" in meta["environment"]
    assert "pid" in meta["environment"]

    assert "git_commit" in meta
