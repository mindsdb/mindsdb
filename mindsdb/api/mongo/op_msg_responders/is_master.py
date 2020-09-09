def match(query):
    return 'isMaster' in query


def response(query):
    response = {
        "ismaster": True,
        "minWireVersion": 0,
        "maxWireVersion": 6,
        "ok": 1
    }
    return response


responder = {
    'match': match,
    'response': response
}
