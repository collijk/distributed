from bdb import BdbQuit


def run_with_debugger(command, *args, **kwargs):
    try:
        command(*args, **kwargs)
    except (BdbQuit, KeyboardInterrupt):
        raise
    except Exception as e:
        import pdb
        import traceback

        traceback.print_exc()
        pdb.post_mortem()

        raise
