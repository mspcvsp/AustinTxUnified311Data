"""Austin 311 Unified Data Utilities"""
from datetime import datetime
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
import copy
import numpy as np
import urllib
import os
import re
from csv import DictReader

def init_db_handle(data_path):
    """Initializes a Couchbase database handle

    Args:,
        data_path: String that stores the full path to folder that contains
                   the Austin Tx Unified 311 open dataset

    Returns:
        cb: Couchbase database handle"""
    authenication = load_authenication(data_path)

    cluster = Cluster('couchbase://localhost')
    authenticator = PasswordAuthenticator(authenication['username'],
                                          authenication['password'])
    cluster.authenticate(authenticator)
    return cluster.open_bucket('AustinTx311Data')

def init_keys(h_csv):
    """Initializes a list that contains Austin Tx Unified 311 data keys

    Args:
        h_csv: Austin Tx Unified 311 data *.csv file handle

    Returns:
        keys: List that contains Austin Tx Unified 311 data keys"""
    header = h_csv.readline().rstrip()
    
    keys = header.split(',')
    
    keys[0] = re.sub(r'\(SR\)', '', keys[0])

    return [re.sub(r'[\s\(\)\.]','',elem).lower() for elem in keys]

def load_authenication(data_path):
    """Loads a Couchbase username and password
    
    Args:
        data_path: String that stores the full path to folder that contains
                   the Austin Tx Unified 311 open dataset

    Returns:
        authenticator: Dictionary that stores a Couchbase username and 
                       password"""
    password_file = os.path.join(data_path,'.password','password.txt')

    with open(password_file, 'rt') as h_file:
        keys = h_file.readline().rstrip().split(',')
        readerobj = DictReader(h_file, keys)
        authentication = readerobj.__next__()

    return authentication

def parse_int(key,
              formatted_record):
    """Parses an integer value

    Args:
        key: String that stores the key for an integer value

        formatted_record: OrderedDict that stores an Austin Tx 311
                          Unified data record

    Returns:
        None - Dictionary is updated in place"""
    if formatted_record[key] == '':
        formatted_record[key] = None
    else:
        try:
            formatted_record[key] = int(formatted_record[key])
        except ValueError:
            formatted_record[key] = None
        
def parse_float(key,
              formatted_record):
    """Parses an floating point value

    Args:
        key: String that stores the key for an floating point value

        formatted_record: OrderedDict that stores an Austin Tx 311
                          Unified data record

    Returns:
        None - Dictionary is updated in place"""
    if formatted_record[key] == '':
        formatted_record[key] = None
    else:
        try:
            formatted_record[key] = float(formatted_record[key])
        except ValueError:
            formatted_record[key] = None

def format_record(record):
    """Formats an Austin Tx Unified 311 data record

    Args:
        record: OrderedDict that stores an Austin Tx Unified 311 
                data record
    
    Returns:
        formatted_record: Tuple that stores a formatted Austin Tx
                          Unified 311 data record"""
    formatted_record = copy.deepcopy(record)
    
    try:
        matchobj = re.match(r'\(([0-9\.-]+), ([0-9\.-]+)\)',
                            formatted_record['latitudelongitude'])

        location = [float(matchobj.group(1)),
                    float(matchobj.group(2))]
    except AttributeError:
        location = [None, None]

    formatted_record['latitudelongitude'] =\
        {'loc': location,
         'title': record['servicerequestnumber']}
    
    for key in ['streetnumber',
                'zipcode',
                'councildistrict']:

        parse_int(key, formatted_record)

    for key in ['stateplanexcoordinate',
                'stateplaneycoordinate',
                'latitudecoordinate',
                'longitudecoordinate']:

        parse_float(key, formatted_record)

    patternobj = re.compile('^([a-z]+)date$')

    for key in ['createddate',
                'statuschangedate',
                'lastupdatedate',
                'closedate']:

        matchobj = patternobj.match(key)
        key_prefix = matchobj.group(1)

        try:
            datetime_value = datetime.strptime(record[key],
                                               '%m/%d/%Y %I:%M:%S %p')

            formatted_record[key_prefix + 'month'] = datetime_value.month
            formatted_record[key_prefix + 'day'] = datetime_value.day
            formatted_record[key_prefix + 'year'] = datetime_value.year
            formatted_record[key_prefix + 'hour'] = datetime_value.hour
        except ValueError:
            formatted_record[key_prefix + 'month'] = None
            formatted_record[key_prefix + 'day'] = None
            formatted_record[key_prefix + 'year'] = None
            formatted_record[key_prefix + 'hour'] = None

        formatted_record.pop(key)

    servicerequestnumber = formatted_record.pop('servicerequestnumber')

    return (servicerequestnumber,
            formatted_record)

def is_empty_record(record):
    """Returns a boolean that keeps track of whether or not a record is
    empty

    Args:
        record: OrderedDict that stores an Austin Tx Unified 311 
                data record
    
    Returns:
        is_emptyrecord_flag: Boolean that keeps track of whether or not
                             a record is empty"""
    return np.sum([elem == '' for elem in record.values()]) ==\
           len(record.values())
