from pystele.engine.exec import ExecEngine

_engine = ExecEngine()

def exec(fn, *, checkpoint_interval_s=None, exec_id=None):
    return _engine.run(
        fn,
        exec_id=exec_id,
        checkpoint_interval_s=checkpoint_interval_s,
    )

def status(exec_id):
    return _engine.status(exec_id)

def kill(exec_id):
    return _engine.kill(exec_id)
