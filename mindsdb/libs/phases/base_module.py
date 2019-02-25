"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""

"""
This class works as an interface to code any module within MindsDB
We use this to standarize the way that code parts of what makes mindsDB in a way such that those can be replaced so long the interfaces remain

The principle is very simple, all you need within the module is set in the method __call__

whatever you pass to call is the input and whatever it returns is the ourput of the module

The interesting part is that in a module you should not instantiate data sources or any global resources
Those should be available via self.session

"""

from mindsdb.libs.constants.mindsdb import *
import time


class BaseModule():

    phase_name = PHASE_END
    log_on_run = True

    def __init__(self, session, transaction, **kwargs):
        '''
        Initialize the base module and the basic global variables
        :param session: the session under which this transaction is happening
        :type transaction: libs.controllers.transaction_controller.TransactionController
        :param transaction: the transaction under which this phases is being called
        :param kwargs: extra arguments passed to run
        '''
        self.kwargs = kwargs
        self.session = session
        self.transaction = transaction # type: controllers.transaction_controller.TransactionController
        self.output = {}
        self.setup(**kwargs)
        self.log = self.transaction.log

    def run(self):
        pass

    def __call__(self, **kwargs):
        start = time.time()
        class_name = type(self).__name__

        if self.phase_name != PHASE_END:
            self.log.warning('Target phase is different than PHASE_END, Only change this for debug purposes')
        # log warning if no phase name has been set or designed for the model
        if self.phase_name == PHASE_END and self.log_on_run:
            self.log.error('Module {class_name} has no \'phase_name\' defined and therefore it cannot be properly tested'.format(class_name=class_name))

        if self.log_on_run:
            self.log.info('[START] {class_name}'.format(class_name=class_name))

        # if we are past the breakpoint do nothing, breakpoints are used when testing a particular module
        if self.transaction.breakpoint is not None and self.phase_name > self.transaction.breakpoint:
            warning = 'Module {class_name} has a phase that is beyond breakpoint, module\'s phase: {current_phase}, transaction\'s breakpoint: {breakpoint}'.format(class_name=class_name, current_phase=self.phase_name, breakpoint=self.transaction.breakpoint)
            self.log.warning(warning)
            exit()


        # else run it
        ret = self.run(**kwargs)
        execution_time = time.time() - start
        if self.log_on_run:
            self.log.info('[END] {class_name}, execution time: {execution_time:.3f} seconds'.format(class_name=class_name, execution_time=execution_time))
        return ret

    def setup(self, **kwargs):
        # This is to be implemented by the child classes
        pass
