
import json




class FirstValueAgg:
    def __init__(self):
        self.first = None

    def step(self, value):
        if self.first == None:
            self.first=value

    def finalize(self):
        return self.first

class ArrayAggJSON:
    def __init__(self):
        self.array = []
        self.limit = 80

    def step(self, value, limit):
        self.array += [value]
        self.limit = limit

    def finalize(self):

        arr =  self.array[-self.limit or None: ]
        if len(arr) < self.limit:
            diff_arra = [None]*(self.limit-len(arr))
            arr = diff_arra + arr
        return json.dumps(arr)
