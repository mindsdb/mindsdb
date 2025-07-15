
class QueryPlan:
    def __init__(self, steps=None, **kwargs):
        self.steps = []
        self.is_resumable = False
        self.is_async = False

        if steps:
            for step in steps:
                self.add_step(step)

    def __eq__(self, other):
        if type(self) != type(other):
            return False

        if len(self.steps) != len(other.steps):
            return False

        for step, other_step in zip(self.steps, other.steps):
            if step != other_step:
                return False

    @property
    def last_step_index(self):
        return len(self.steps) - 1

    def add_step(self, step):
        if not step.step_num:
            step.step_num = len(self.steps)
        self.steps.append(step)
        return self.steps[-1]
