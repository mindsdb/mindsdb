import platform


def get_mp_context():
    if platform.system() in ['Linux', 'Darwin']:
        return 'fork'
    else:
        return spawn
