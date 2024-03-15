import json

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
