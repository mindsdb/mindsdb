#!/usr/bin/env python
"""
Function for deterministically generating a single hash for a directory of files,
taking into account only file contents and not filenames.

Usage:

$ dirhash('/path/to/directory', 'md5')

"""


import argparse

import checksumdir

VERSION = checksumdir.__version__


def main():
    parser = argparse.ArgumentParser(description="Determine the hash for directory.")
    parser.add_argument(
        "-v", "--version", action="version", version="checksumdir %s" % VERSION
    )
    parser.add_argument("directory", help="Directory for which to generate hash.")
    parser.add_argument(
        "-a", "--algorithm", choices=("md5", "sha1", "sha256", "sha512"), default="md5"
    )
    parser.add_argument(
        "-e", "--excluded-files", nargs="+", help="List of excluded files."
    )
    parser.add_argument(
        "-i",
        "--ignore-hidden",
        action="store_true",
        default=False,
        help="Ignore hidden files",
    )
    parser.add_argument(
        "-f",
        "--follow-links",
        action="store_true",
        default=False,
        help="Follow soft links",
    )
    parser.add_argument(
        "-x",
        "--excluded-extensions",
        nargs="+",
        help="List of excluded file extensions.",
    )
    parser.add_argument(
        "-p", 
        "--include-paths", 
        action="store_true", 
        default=False,
        help="Include file path in the hash")

    args = parser.parse_args()
    print(
        checksumdir.dirhash(
            dirname=args.directory,
            hashfunc=args.algorithm,
            excluded_files=args.excluded_files,
            ignore_hidden=args.ignore_hidden,
            followlinks=args.follow_links,
            excluded_extensions=args.excluded_extensions,
            include_paths=args.include_paths
        )
    )


if __name__ == "__main__":
    main()
