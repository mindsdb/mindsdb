import json

from flask import Response


def http_error(status_code, title, detail=None):
    ''' Wrapper for error responce acoording with RFC 7807 (https://tools.ietf.org/html/rfc7807)

        :param status_code: int - http status code for response
        :param title: str
        :param detail: str

        :return: flask Response object
    '''
    if detail is None:
        if 400 <= status_code < 500:
            detail = "A client error occurred. Please check your request and try again."
        elif 500 <= status_code < 600:
            detail = "A server error occurred. Please try again later."
        else:
            detail = "An error occurred while processing the request. Please try again later."

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
