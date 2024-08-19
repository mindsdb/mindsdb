import logging

from waitress.server import create_server


def serve(app, **kw):
    _server = kw.pop("_server", create_server)  # test shim
    _quiet = kw.pop("_quiet", False)  # test shim
    _profile = kw.pop("_profile", False)  # test shim
    if not _quiet:  # pragma: no cover
        # idempotent if logging has already been set up
        logging.basicConfig()
    server = _server(app, **kw)
    if not _quiet:  # pragma: no cover
        server.print_listen("Serving on http://{}:{}")
    if _profile:  # pragma: no cover
        profile("server.run()", globals(), locals(), (), False)
    else:
        server.run()


def serve_paste(app, global_conf, **kw):
    serve(app, **kw)
    return 0


def profile(cmd, globals, locals, sort_order, callers):  # pragma: no cover
    # runs a command under the profiler and print profiling output at shutdown
    import os
    import profile
    import pstats
    import tempfile

    fd, fn = tempfile.mkstemp()
    try:
        profile.runctx(cmd, globals, locals, fn)
        stats = pstats.Stats(fn)
        stats.strip_dirs()
        # calls,time,cumulative and cumulative,calls,time are useful
        stats.sort_stats(*(sort_order or ("cumulative", "calls", "time")))
        if callers:
            stats.print_callers(0.3)
        else:
            stats.print_stats(0.3)
    finally:
        os.remove(fn)
