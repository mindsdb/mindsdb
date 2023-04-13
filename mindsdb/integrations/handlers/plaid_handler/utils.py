def parse_transaction(res:list):
    parsed=[]
    for dic in res:
        dic=dic.to_dict()
        # location=dic['location']
        # del dic['payment_meta'], dic['location']
        # req_dict = dic
        # del req_dict['category']

        # for key in payment_meta.keys():
        #     req_dict[key]=payment_meta[key]

        # for key in location.keys():
        #     req_dict[key]=location[key]
        
        parsed.append(dic)
        
    return parsed


