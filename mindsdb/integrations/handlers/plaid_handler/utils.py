def parse_transaction(res: list):
    parsed = []
    for dic in res:
        dic = dic.to_dict()
        parsed.append(dic)

    return parsed
