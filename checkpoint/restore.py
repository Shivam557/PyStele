import os
import json
import hashlib
import msgpack

from .exceptions import ChecksumMismatchError, CorruptCheckpointError


def restore_checkpoint(
    checkpoint_path: str,
    target_namespace: dict,
    prefix: str | None = None,
):
    """
    Restore a checkpoint into target_namespace.
    Returns list of restored variable names.
    """
    try:
        with open(os.path.join(checkpoint_path, "manifest.json")) as f:
            manifest = json.load(f)

        with open(os.path.join(checkpoint_path, "metadata.json")) as f:
            metadata = json.load(f)

        with open(os.path.join(checkpoint_path, "objects.bin"), "rb") as f:
            data = f.read()

        with open(os.path.join(checkpoint_path, "checksum.sha256")) as f:
            expected = f.read().strip()

    except FileNotFoundError as e:
        raise CorruptCheckpointError(str(e))

    # verify checksum BEFORE deserialization
    h = hashlib.sha256()
    h.update(json.dumps(manifest, sort_keys=True).encode())
    h.update(json.dumps(metadata, sort_keys=True).encode())
    h.update(data)

    if h.hexdigest() != expected:
        raise ChecksumMismatchError("Checkpoint checksum mismatch")

    objects = msgpack.unpackb(data, raw=False)

    restored = []
    for k, v in objects.items():
        name = f"{prefix}{k}" if prefix else k
        target_namespace[name] = v
        restored.append(name)

    return restored
