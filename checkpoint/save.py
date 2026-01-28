import os
import json
import hashlib
import tempfile
import shutil
import msgpack
from datetime import datetime, timezone

from .exceptions import UnserializableError, AtomicWriteError


SAFE_PRIMITIVES = (int, float, bool, str, type(None), bytes)


def _is_safe(obj):
    if isinstance(obj, SAFE_PRIMITIVES):
        return True
    if isinstance(obj, list):
        return all(_is_safe(x) for x in obj)
    if isinstance(obj, tuple):
        return all(_is_safe(x) for x in obj)
    if isinstance(obj, dict):
        return all(
            isinstance(k, str) and _is_safe(v)
            for k, v in obj.items()
        )
    return False


def save_checkpoint(
    execution_id: str,
    namespace: dict,
    path: str,
    name: str | None = None,
    include: list | None = None,
) -> str:
    """
    Save a deterministic, atomic checkpoint of safe objects from namespace.
    Returns checkpoint_id (sha256 hex).
    """
    os.makedirs(path, exist_ok=True)

    # select variables
    if include is None:
        items = dict(namespace)
    else:
        items = {k: namespace[k] for k in include if k in namespace}

    # validate
    errors = []
    for k, v in items.items():
        if not _is_safe(v):
            errors.append((k, type(v).__name__, repr(v)[:80]))

    if errors:
        raise UnserializableError(errors)

    # deterministic order
    ordered = {k: items[k] for k in sorted(items)}

    # serialize objects
    packed = msgpack.packb(ordered, use_bin_type=True)

    manifest = {
        "variables": list(ordered.keys()),
        "schema": "v1",
    }

    metadata = {
        "execution_id": execution_id,
        "checkpoint_name": name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # compute checksum
    h = hashlib.sha256()
    h.update(json.dumps(manifest, sort_keys=True).encode())
    h.update(json.dumps(metadata, sort_keys=True).encode())
    h.update(packed)
    checkpoint_id = h.hexdigest()

    final_dir = os.path.join(path, checkpoint_id)
    tmp_dir = tempfile.mkdtemp(prefix="_ckpt_", dir=path)

    try:
        with open(os.path.join(tmp_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f, sort_keys=True)

        with open(os.path.join(tmp_dir, "metadata.json"), "w") as f:
            json.dump(metadata, f, sort_keys=True)

        with open(os.path.join(tmp_dir, "objects.bin"), "wb") as f:
            f.write(packed)

        with open(os.path.join(tmp_dir, "checksum.sha256"), "w") as f:
            f.write(checkpoint_id)

        os.replace(tmp_dir, final_dir)

    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise AtomicWriteError(str(e))

    return checkpoint_id
