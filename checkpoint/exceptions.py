class CheckpointError(Exception):
    """Base class for checkpoint errors."""


class UnserializableError(CheckpointError):
    def __init__(self, details):
        msg = "Unserializable objects detected:\n"
        for var, typ, reason in details:
            msg += f"  - {var}: {typ} ({reason})\n"
        super().__init__(msg)
        self.details = details


class ChecksumMismatchError(CheckpointError):
    pass


class CorruptCheckpointError(CheckpointError):
    pass


class AtomicWriteError(CheckpointError):
    pass
