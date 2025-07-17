class Result:
    """A placeholder for cached results of some previous plan step"""
    def __init__(self, step_num):
        self.step_num = step_num

    def __hash__(self):
        return 'Result' + self.step_num.__hash__()

    def __eq__(self, other):
        if isinstance(other, Result):
            return self.step_num == other.step_num
        return False

    @property
    def ref_name(self):
        return f'result_{self.step_num}'

    def __repr__(self):
        return f'Result(step={self.step_num})'
