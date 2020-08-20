import argparse


def args_parse():
    parser = argparse.ArgumentParser(description='CL argument for mindsdb server')
    parser.add_argument('--api', type=str, default=None)
    parser.add_argument('--config', type=str, default=None)
    return parser.parse_args()
