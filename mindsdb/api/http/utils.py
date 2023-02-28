import json
import os

from flask import Response


def http_error(status_code, title, detail=''):
    ''' Wrapper for error responce acoording with RFC 7807 (https://tools.ietf.org/html/rfc7807)

        :param status_code: int - http status code for response
        :param title: str
        :param detail: str

        :return: flask Response object
    '''
    return Response(
        response=json.dumps({
            'title': title,
            'detail': detail
        }),
        status=status_code,
        headers={
            'Content-Type': 'application/problem+json'
        }
    )


def __is_within_directory(directory, target):
    abs_directory = os.path.abspath(directory)
    abs_target = os.path.abspath(target)
    prefix = os.path.commonprefix([abs_directory, abs_target])
    return prefix == abs_directory


def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
    for member in tar.getmembers():
        member_path = os.path.join(path, member.name)
        if not __is_within_directory(path, member_path):
            raise Exception("Attempted Path Traversal in Tar File")
    tar.extractall(path, members, numeric_owner)
