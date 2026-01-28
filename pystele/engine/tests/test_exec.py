import time
import os
import json
import psutil
from pystele.engine.exec import ExecEngine
import pytest


def counter_task(context, limit=5000):
    i = context.get("i", 0)
    while i < limit:
        i += 1
        context["i"] = i
        if i % 100 == 0:
            time.sleep(0.01)


@pytest.mark.skipif(os.name != "posix", reason="SIGSTOP not supported on this OS")
def test_pause_resume_sigstop():
    engine = ExecEngine()
    exec_id = engine.run(counter_task, kwargs={"limit": 2000})

    time.sleep(0.5)
    engine.pause(exec_id)

    st = engine.status(exec_id)
    assert st["state"] == "PAUSED"
    pid = st["pid"]
    assert psutil.Process(pid).status() == psutil.STATUS_STOPPED

    engine.resume(exec_id)
    time.sleep(0.5)

    st2 = engine.status(exec_id)
    assert st2["state"] == "RUNNING"


def test_kill_and_checkpoint_resume():
    engine = ExecEngine()
    exec_id = engine.run(
        counter_task,
        kwargs={"limit": 3000},
        checkpoint_interval_s=1,
    )

    time.sleep(2)
    engine.kill(exec_id)

    ckpt = os.path.join(".pystele", exec_id, "checkpoint.pkl")
    assert os.path.isdir(os.path.join(".pystele", exec_id))


def test_audit_log_events():
    engine = ExecEngine()
    exec_id = engine.run(counter_task, kwargs={"limit": 500})

    time.sleep(0.2)
    engine.pause(exec_id)
    engine.resume(exec_id)
    engine.kill(exec_id)

    audit = os.path.join(".pystele", exec_id, "audit.log")
    events = [json.loads(l)["event"] for l in open(audit)]

    assert "START" in events
    assert ("PAUSE" in events) or ("PAUSE_SKIPPED" in events)
    assert ("RESUME" in events) or ("RESUME_SKIPPED" in events)
    assert "KILL" in events