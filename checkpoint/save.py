import os
import json
import hashlib
import tempfile
import shutil
import msgpack
from datetime import datetime, timezone
import io
import inspect
import sys
import subprocess


from .exceptions import UnserializableError, AtomicWriteError


# Optional NumPy support (CPU only)
try:
    import numpy as np
except ImportError:
    np = None


# -----------------------------
# Safe primitive types
# -----------------------------
SAFE_PRIMITIVES = (int, float, bool, str, type(None), bytes)


def _is_numpy_array(obj):
    if np is None:
        return False
    return isinstance(obj, np.ndarray)


def _is_safe(obj):
    # NumPy arrays (CPU only)
    if _is_numpy_array(obj):
        if hasattr(obj, "device") and str(obj.device) != "cpu":
            return False
        return True

    if isinstance(obj, SAFE_PRIMITIVES):
        return True

    if isinstance(obj, list):
        return all(_is_safe(x) for x in obj)

    if isinstance(obj, tuple):
        return all(_is_safe(x) for x in obj)

    if isinstance(obj, dict):
        return all(isinstance(k, str) and _is_safe(v) for k, v in obj.items())

    return False


def _serialize_numpy_array(arr):
    buf = io.BytesIO()
    np.save(buf, arr, allow_pickle=False)
    return buf.getvalue()


def _serialize(obj):
    if _is_numpy_array(obj):
        return _serialize_numpy_array(obj)
    return msgpack.packb(obj, use_bin_type=True)


def save_checkpoint(
    execution_id: str,
    namespace: dict,
    path: str,
    name: str | None = None,
    include: list | None = None,
) -> str:
    os.makedirs(path, exist_ok=True)

    # -----------------------------
    # Capture caller info (Day-4)
    # -----------------------------
    frame = inspect.stack()[1]
    caller_info = {
        "file": frame.filename,
        "function": frame.function,
        "line": frame.lineno,
    }
    env_info = {
    "python_version": sys.version,
    "pid": os.getpid(),
}
    
    def _get_git_commit():
        try:
            out = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                stderr=subprocess.DEVNULL,
                timeout=1,
            )
            return out.decode().strip()
        except Exception:
            return None


    # -----------------------------
    # 1. Select variables
    # -----------------------------
    if include is None:
        items = dict(namespace)
    else:
        items = {k: namespace[k] for k in include if k in namespace}

    # -----------------------------
    # 2. Validate safety
    # -----------------------------
    errors = []
    for k, v in items.items():
        if not _is_safe(v):
            errors.append((k, type(v).__name__, repr(v)[:80]))

    if errors:
        raise UnserializableError(errors)

    # -----------------------------
    # 3. Deterministic order
    # -----------------------------
    ordered_keys = sorted(items.keys())

    # -----------------------------
    # 4. Build objects.bin + idx
    # -----------------------------
    objects_blob = bytearray()
    objects_idx = {}

    offset = 0
    for key in ordered_keys:
        value = items[key]
        data = _serialize(value)
        length = len(data)
        sha = hashlib.sha256(data).hexdigest()

        objects_blob.extend(data)

        objects_idx[key] = {
            "offset": offset,
            "length": length,
            "sha256": sha,
            "type": "numpy" if _is_numpy_array(value) else "msgpack",
        }

        offset += length

    # -----------------------------
    # 5. Manifest & metadata
    # -----------------------------
    manifest = {
        "variables": ordered_keys,
        "schema": "v1",
    }


    # Get git commit hash (if available)
    try:
        git_commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            timeout=1,
        ).decode().strip()
    except Exception:
        git_commit = None


    metadata = {
        "execution_id": execution_id,
        "checkpoint_name": name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "caller": caller_info,
        "environment": env_info,
        "git_commit": git_commit,
    }

    # -----------------------------
    # 6. Compute checkpoint ID
    # -----------------------------
    h = hashlib.sha256()
    h.update(json.dumps(manifest, sort_keys=True).encode())
    h.update(objects_blob)
    checkpoint_id = h.hexdigest()

    final_dir = os.path.join(path, checkpoint_id)

    if os.path.exists(final_dir):
        return checkpoint_id

    tmp_dir = tempfile.mkdtemp(prefix="_ckpt_", dir=path)

    try:
        # -----------------------------
        # 7. Write files (atomic)
        # -----------------------------
        def _write_json(fname, obj):
            with open(fname, "w") as f:
                json.dump(obj, f, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())

        _write_json(os.path.join(tmp_dir, "manifest.json"), manifest)
        _write_json(os.path.join(tmp_dir, "metadata.json"), metadata)
        _write_json(os.path.join(tmp_dir, "objects.idx"), objects_idx)

        with open(os.path.join(tmp_dir, "objects.bin"), "wb") as f:
            f.write(objects_blob)
            f.flush()
            os.fsync(f.fileno())

        with open(os.path.join(tmp_dir, "checksum.sha256"), "w") as f:
            f.write(checkpoint_id)
            f.flush()
            os.fsync(f.fileno())

        try:
            dir_fd = os.open(tmp_dir, os.O_RDONLY)
            os.fsync(dir_fd)
            os.close(dir_fd)
        except Exception:
            pass

        os.replace(tmp_dir, final_dir)

    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise AtomicWriteError(str(e))

    return checkpoint_id
