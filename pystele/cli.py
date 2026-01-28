import argparse
from datetime import datetime
from pystele.engine.exec import ExecEngine

engine = ExecEngine()

def main():
    parser = argparse.ArgumentParser(prog="pystele")
    sub = parser.add_subparsers(dest="cmd", required=True)

    run_p = sub.add_parser("run")
    run_p.add_argument("module", help="python module:file:function")

    status_p = sub.add_parser("status")
    status_p.add_argument("exec_id")

    kill_p = sub.add_parser("kill")
    kill_p.add_argument("exec_id")

    pause_p = sub.add_parser("pause")
    pause_p.add_argument("exec_id")

    resume_p = sub.add_parser("resume")
    resume_p.add_argument("exec_id")

    ls_p = sub.add_parser("ls")


    args = parser.parse_args()

    if args.cmd == "run":
        mod, fn = args.module.split(":")
        module = __import__(mod, fromlist=[fn])
        func = getattr(module, fn)
        eid = engine.run(func)
        print(eid)

    elif args.cmd == "status":
        # print(engine.status(args.exec_id))
        st = engine.status(args.exec_id)
        print(f"exec_id : {st['exec_id']}")
        print(f"state   : {st['state']}")
        print(f"pid     : {st['pid']}")


    elif args.cmd == "kill":
        engine.kill(args.exec_id)
        print("killed", args.exec_id)

    elif args.cmd == "pause":
        engine.pause(args.exec_id)
        print("paused", args.exec_id)

    elif args.cmd == "resume":
        engine.resume(args.exec_id)
        print("resumed", args.exec_id)

    elif args.cmd == "ls":
        jobs = engine.list()
        if not jobs:
            print("no executions found")
            return

        print(f"{'EXEC_ID':<36}  {'STATE':<10}  PID")
        print("-" * 55)
        for eid, st in jobs.items():
            pid = st["pid"] if st["pid"] else "-"
            print(f"{eid:<36}  {st['state']:<10}  {pid}")
