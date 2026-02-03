import os
import json
import hashlib
import msgpack
import io

from .exceptions import ChecksumMismatchError, CorruptCheckpointError

# Optional NumPy support
try:
    import numpy as np
except ImportError:
    np = None


def restore_checkpoint(
    checkpoint_path: str,
    target_namespace: dict,
    prefix: str | None = None,
):
    """
    Restore a checkpoint into target_namespace.
    Verifies integrity before deserialization.
    Returns list of restored variable names.
    """

    # -----------------------------
    # 1. Load required files
    # -----------------------------
    try:
        with open(os.path.join(checkpoint_path, "manifest.json")) as f:
            manifest = json.load(f)

        with open(os.path.join(checkpoint_path, "metadata.json")) as f:
            metadata = json.load(f)

        with open(os.path.join(checkpoint_path, "objects.idx")) as f:
            objects_idx = json.load(f)

        with open(os.path.join(checkpoint_path, "objects.bin"), "rb") as f:
            objects_blob = f.read()

        with open(os.path.join(checkpoint_path, "checksum.sha256")) as f:
            expected_checksum = f.read().strip()

    except FileNotFoundError as e:
        raise CorruptCheckpointError(f"Missing checkpoint file: {e}")

    # -----------------------------
    # 2. Verify checkpoint checksum
    # -----------------------------
    h = hashlib.sha256()
    h.update(json.dumps(manifest, sort_keys=True).encode())
    h.update(objects_blob)

    if h.hexdigest() != expected_checksum:
        raise ChecksumMismatchError("Checkpoint checksum mismatch")

    # -----------------------------
    # 3. Restore variables
    # -----------------------------
    restored = []

    for var in manifest["variables"]:
        if var not in objects_idx:
            raise CorruptCheckpointError(
                f"Missing index entry for variable '{var}'"
            )

        entry = objects_idx[var]
        offset = entry["offset"]
        length = entry["length"]
        expected_sha = entry["sha256"]

        data = objects_blob[offset : offset + length]

        # Per-object integrity check
        actual_sha = hashlib.sha256(data).hexdigest()
        if actual_sha != expected_sha:
            raise ChecksumMismatchError(
                f"Object '{var}' failed integrity check"
            )

        # -----------------------------
        # Deserialize based on type
        # -----------------------------
        if entry.get("type") == "numpy":
            if np is None:
                raise CorruptCheckpointError(
                    "NumPy not available to restore array"
                )
            value = np.load(io.BytesIO(data), allow_pickle=False)
        else:
            value = msgpack.unpackb(data, raw=False)

        name = f"{prefix}{var}" if prefix else var
        target_namespace[name] = value
        restored.append(name)

    return restored
