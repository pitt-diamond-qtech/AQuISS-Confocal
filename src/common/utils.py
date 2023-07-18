# Created by Gurudev Dutt <gdutt@pitt.edu> on 2023-07-17



from pathlib import Path
import functools, logging
import os, inspect
import psycopg2 as pg2

def get_project_root() -> Path: # new feature in Python 3.x i.e. annotations
    """Returns project root folder."""
    return Path(__file__).parent.parent

#


def get_module_name():
    fname = inspect.stack()[-1].filename
   #print(fname)
    p = Path(fname)
    return p.parts[-1].split('.')[0]


def create_logger(loggername):
    rootdir = get_project_root()
    logfiledir = rootdir / 'logs/'
    if not logfiledir.exists():
        os.mkdir(logfiledir)
        print('Creating directory for logging at:'.format(logfiledir.resolve()))

    log = logging.getLogger(loggername)
    log.setLevel(logging.DEBUG)
    # create a file handler that logs even debug messages
    fh = logging.FileHandler((logfiledir / str(loggername+ '.log')).resolve())
    fh.setLevel(logging.DEBUG)
    # create a console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    log.addHandler(fh)
    log.addHandler(ch)
    return log


# this code is adapted from https://wiki.python.org/moin/PythonDecoratorLibrary#Logging_decorator_with_specified_logger_
# .28or_default.29


class log_with(object):
    '''Logging decorator that allows you to log with a specific logger.
    '''
    # Customize these messages
    ENTRY_MESSAGE = 'Entering {}'
    EXIT_MESSAGE = 'Exiting {}'

    def __init__(self, logger=None):
        self.logger = logger

    def __call__(self, func):
        '''Returns a wrapper that wraps func. The wrapper will log the entry and exit points of the function
        with logging.INFO level.
        '''
        # set logger if it was not set earlier
        if not self.logger:
            #logging.basicConfig()
            modname = get_module_name() #func.__module__
            self.logger = create_logger(modname)


        @functools.wraps(func)
        def wrapper(*args, **kwds):
            self.logger.info(self.ENTRY_MESSAGE.format(func.__name__))  # logging level .info(). Set to .debug() if you want to
            f_result = func(*args, **kwds)
            self.logger.info(self.EXIT_MESSAGE.format(func.__name__))   # logging level .info(). Set to .debug() if you want to
            return f_result
        return wrapper

def db_insert(original_function):
    """

    This function is a decorator for any function that would insert data
    into the DB and is designed in the Query/Content Model.

    """
    def wrapper_function(*args,**kwargs):
        a,b = original_function(*args,**kwargs)
        conn = pg2.connect(database= 'aquiss-confocal', user='postgres', password='Argonne')
        cur = conn.cursor()
        executable = cur.mogrify(a,b)
        cur.execute(executable)
        conn.commit()
        conn.close()
    return wrapper_function

@db_insert
def insert_data_confocal(params):

    """
        This function is used to upload data into the SQL DB.

        :param: Takes class params and set of params to be inserted.
        :type: list
        :rtype: string,list
        :return: returns a query string and a list of contents.

    """

    query = 'INSERT INTO CFdata(date,data_id,sig_data,ref_data,sample,count_time,reset_time,avg,threshold,aom_delay,' \
            'mw_delay,type,start,stepsize,steps,pts,srs,avgcount,x_arr,sample_name,nv_name,waveguide,nv_depth,nv_counts,metadata,exp,time_stamp) ' \
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)'

    content = (params[0],params[1],params[2],params[3],params[4],
               params[5],params[6],params[7],params[8],params[9],
               params[10],params[11],params[12],params[13],params[14],
               params[15],params[16],params[17],params[18],params[19],
               params[20],params[21],params[22],params[23],params[24],params[25])

    return (query,content)

