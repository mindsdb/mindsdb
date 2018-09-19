
class TesterResponse():

    def __init__(self, error=0, accuracy =0 , predicted_targets={}, real_targets={}):
        self.error = error
        self.accuracy = accuracy
        self.predicted_targets = predicted_targets
        self.real_targets = real_targets

