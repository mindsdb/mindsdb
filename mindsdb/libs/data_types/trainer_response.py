
class TrainerResponse():

    def __init__(self, model, epoch=0, batch=0, loss=0):
        self.model = model
        self.epoch = epoch
        self.batch = batch
        self.loss = loss

