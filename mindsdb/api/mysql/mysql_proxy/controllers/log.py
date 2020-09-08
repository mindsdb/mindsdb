import os
import logging
from logging.handlers import RotatingFileHandler

def init_logger(config, nolog=False):
    config = config['api']['mysql']['log']

    '''
    if not os.path.exists(config['folder']):
        os.makedirs(config['folder'])
    '''
    
    logger = logging.getLogger('mindsdb_sql')

    logger.setLevel(min(
        getattr(logging, config['file_level']),
        getattr(logging, config['console_level'])
    ))
    '''
    fh = RotatingFileHandler(
        os.path.join(config['folder'], config['file']),
        mode='a',
        encoding=config.get('encoding', 'utf-8'),
        maxBytes=100*1024,
        backupCount=3
    )
    fh.setLevel(config['file_level'])
    '''
    # create console handler
    ch = logging.StreamHandler()
    ch.setLevel(config['console_level'])

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    #fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    #logger.addHandler(fh)
    logger.addHandler(ch)

log = logging.getLogger('mindsdb_sql')
