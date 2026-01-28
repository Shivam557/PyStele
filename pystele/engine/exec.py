"""
ExecEngine — Minimal Durable Execution Engine (MVP)

Linux: full pause/resume via SIGSTOP / SIGCONT
Windows/macOS: checkpoint + kill + audit (no OS-level pause)

Target: Linux-first, Windows-safe fallback
"""

from typing import Callable, Any, Optional, Dict
import os
import sys
import time
import json
import uuid
import pickle
import signal
import logging
import traceback
import multiprocessing as mp
from datetime import datetime
from filelock import FileLock
import psutil

BASE_DIR = os.path.abspath(".pystele")
IS_POSIX = os.name == "posix"


# ------------------------
# Utilities
# ------------------------

def _now() -> float:
    return time.time()


def _ts() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _atomic_write(path: str, data: bytes) -> None:
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def checkpoint_state(obj: Any, path: str) -> Optional[str]:
    try:
        data = pickle.dumps(obj)
        _atomic_write(path, data)
        return None
    except Exception as e:
        return f"Checkpoint failed: {e}"


# ------------------------
# Child process
# ------------------------

def _child_entry(
    exec_id: str,
    func: Callable[..., Any],
    args: tuple,
    kwargs: dict,
    work_dir: str,
    checkpoint_interval_s: Optional[int],
):
    stdout_path = os.path.join(work_dir, "stdout.log")
    stderr_path = os.path.join(work_dir, "stderr.log")
    checkpoint_path = os.path.join(work_dir, "checkpoint.pkl")
    audit_path = os.path.join(work_dir, "audit.log")

    sys.stdout = open(stdout_path, "a", buffering=1)
    sys.stderr = open(stderr_path, "a", buffering=1)

    def audit(event: str, meta: Optional[Dict] = None):
        rec = {
            "ts": _ts(),
            "event": event,
            "pid": os.getpid(),
            "meta": meta or {},
        }
        with open(audit_path, "a") as f:
            f.write(json.dumps(rec) + "\n")
            f.flush()

    context = {}
    if os.path.exists(checkpoint_path):
        try:
            with open(checkpoint_path, "rb") as f:
                context = pickle.load(f)
            audit("CHECKPOINT_LOADED")
        except Exception as e:
            audit("ERROR", {"error": str(e)})

    last_ckpt = _now()

    def maybe_checkpoint():
        nonlocal last_ckpt
        if checkpoint_interval_s is None:
            return
        if _now() - last_ckpt >= checkpoint_interval_s:
            err = checkpoint_state(context, checkpoint_path)
            if err:
                audit("ERROR", {"error": err})
            else:
                audit("CHECKPOINT")
            last_ckpt = _now()

    try:
        audit("START")
        func(context, *args, **kwargs)
        maybe_checkpoint()
        audit("EXIT")
    except Exception:
        audit("ERROR", {"traceback": traceback.format_exc()})
        raise
    finally:
        sys.stdout.flush()
        sys.stderr.flush()


# ------------------------
# ExecEngine
# ------------------------

class ExecEngine:
    def __init__(self, base_dir: str = BASE_DIR):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        logging.basicConfig(level=logging.INFO)

    # ---- helpers ----

    def _exec_dir(self, exec_id: str) -> str:
        return os.path.join(self.base_dir, exec_id)

    def _meta_path(self, exec_id: str) -> str:
        return os.path.join(self._exec_dir(exec_id), "meta.json")

    def _audit_path(self, exec_id: str) -> str:
        return os.path.join(self._exec_dir(exec_id), "audit.log")

    def _pid_path(self, exec_id: str) -> str:
        return os.path.join(self._exec_dir(exec_id), "pid")

    def _checkpoint_path(self, exec_id: str) -> str:
        return os.path.join(self._exec_dir(exec_id), "checkpoint.pkl")

    def _write_audit(self, exec_id: str, event: str, meta: Optional[Dict] = None):
        rec = {
            "ts": _ts(),
            "event": event,
            "pid": self._read_pid(exec_id),
            "meta": meta or {},
        }
        with open(self._audit_path(exec_id), "a") as f:
            f.write(json.dumps(rec) + "\n")
            f.flush()

    def _write_meta(self, exec_id: str, meta: Dict):
        path = self._meta_path(exec_id)
        with FileLock(path + ".lock"):
            _atomic_write(path, json.dumps(meta, indent=2).encode())

    def _read_meta(self, exec_id: str) -> Dict:
        try:
            with open(self._meta_path(exec_id)) as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def _write_pid(self, exec_id: str, pid: int):
        _atomic_write(self._pid_path(exec_id), str(pid).encode())

    def _read_pid(self, exec_id: str) -> Optional[int]:
        try:
            return int(open(self._pid_path(exec_id)).read())
        except Exception:
            return None

    # ---- API ----

    def run(
        self,
        func: Callable[..., Any],
        args: tuple = (),
        kwargs: dict = {},
        exec_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
        checkpoint_interval_s: Optional[int] = None,
    ) -> str:
        exec_id = exec_id or uuid.uuid4().hex
        work_dir = self._exec_dir(exec_id)
        os.makedirs(work_dir, exist_ok=True)

        meta = {
            "exec_id": exec_id,
            "state": "RUNNING",
            "created_at": _ts(),
            "metadata": metadata or {},
            "checkpoint_interval_s": checkpoint_interval_s,
        }
        self._write_meta(exec_id, meta)

        p = mp.Process(
            target=_child_entry,
            args=(exec_id, func, args, kwargs, work_dir, checkpoint_interval_s),
            daemon=False,
        )
        p.start()

        self._write_pid(exec_id, p.pid)
        self._write_audit(exec_id, "START")

        return exec_id

    def pause(self, exec_id: str, reason: Optional[str] = None) -> None:
        if not IS_POSIX:
            self._write_audit(exec_id, "PAUSE_SKIPPED", {"reason": "os_not_supported"})
            return

        pid = self._read_pid(exec_id)
        if pid and psutil.pid_exists(pid):
            os.kill(pid, signal.SIGSTOP)
            self._write_audit(exec_id, "PAUSE")

    def resume(self, exec_id: str) -> None:
        if not IS_POSIX:
            self._write_audit(exec_id, "RESUME_SKIPPED", {"reason": "os_not_supported"})
            return

        pid = self._read_pid(exec_id)
        if pid and psutil.pid_exists(pid):
            os.kill(pid, signal.SIGCONT)
            self._write_audit(exec_id, "RESUME")
            return

        raise RuntimeError("Process is not running and restart is not supported in MVP")

    def kill(self, exec_id: str) -> None:
        pid = self._read_pid(exec_id)
        if pid and psutil.pid_exists(pid):
            try:
                # os.kill(pid, signal.SIGKILL)
                p = psutil.Process(pid)
                p.kill()
            except psutil.NoSuchProcess:
                pass
        self._write_audit(exec_id, "KILL")

    def status(self, exec_id: str) -> Dict:
        pid = self._read_pid(exec_id)
        state = "STOPPED"

        if pid and psutil.pid_exists(pid):
            p = psutil.Process(pid)
            if p.status() == psutil.STATUS_STOPPED:
                state = "PAUSED"
            else:
                state = "RUNNING"

        return {
            "exec_id": exec_id,
            "state": state,
            "pid": pid,
        }

    def list(self) -> Dict[str, Dict]:
        return {
            eid: self.status(eid)
            for eid in os.listdir(self.base_dir)
        }


if __name__ == "__main__":
    print("ExecEngine loaded.")
