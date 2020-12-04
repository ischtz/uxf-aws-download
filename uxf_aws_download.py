import os
import sys
import time
import argparse

import pandas as pd

import boto3
from botocore.config import Config


def get_table_name(experiment, table, prefix='UXFData'):
    """ Build UXF table name string """
    return '{:s}.{:s}.{:s}'.format(prefix, experiment, table)


def get_output_file_name(experiment, table):
    """ Build CSV file name, including timestamp """
    t = time.strftime('%Y%m%d_%H%m', time.localtime())
    return '{:s}_{:s}_{:s}.csv'.format(experiment, table, t)


def scan_table_to_df(dynamodb, table_name, unpack=True):
    """ Scan entire data from a given table into a DataFrame.
    
    Note: For some tables, UXF stores data in one row per session
    and nested item lists per trial. Setting unpack=True will 
    unpack this data into rows using pandas.DataFrame.explode().

    Args:
        dynamodb: AWS DynamoDB resource instance 
        table_name (str): Name of table to retrieve
        unpack (bool): if True, unpack nested entries
    
    Returns: Pandas.DataFrame containing results
    """
    table = dynamodb.Table(table_name)
    response = table.scan()
    items = response['Items']
    
    # Keep scanning until all data is retrieved
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    res = pd.DataFrame.from_records(items)
    if unpack and res.shape[0] > 0:
        res = res.set_index(['ppid_session_dataname']).apply(pd.Series.explode).reset_index()

    return res


def save_dataframe(df, file_name, sep='\t'):
    """ Saves a retrieved DataFrame to file. """
    if df.shape[0] > 0:
        df.to_csv(file_name, sep=sep, index=False)
        print('{:s}: {:d} rows.'.format(file_name, df.shape[0]))
    else:
        print('{:s}: no data, skipping.'.format(file_name))


def download_uxf_tables(dynamodb, experiment, tables, prefix='UXFData'):
    """ Scan multiple DynamoDB tables and save to CSV 
    
    Args:
        dynamodb: AWS DynamoDB resource instance 
        experiment (str): UXF Experiment Name to download
        tables (list): List of UXF table names
        prefix (str): Prefix used for DynamoDB tables
    """
    tnames = [get_table_name(experiment, t, prefix) for t in tables]
    fnames = [get_output_file_name(experiment, t) for t in tables]
    for tbl, out in zip(tnames, fnames):
        try:
            data = scan_table_to_df(dynamodb, tbl)
            save_dataframe(data, out)

        except dynamodb.meta.client.exceptions.ResourceNotFoundException:
            print('{:s}: table {:s} not found, skipping.'.format(out, tbl))


def download_tracker_data(dynamodb, experiment, prefix='UXFData'):
    """ Download position and orientation tracker data into CSV file
    
    Tracker data currently needs a slightly different unpacking process,
    because the trial_num field is not written once per trial.

    Args:
        dynamodb: AWS DynamoDB resource instance 
        experiment (str): UXF Experiment Name to download
        prefix (str): Prefix used for DynamoDB tables
    
    Returns: Pandas.DataFrame of Tracker data
    """
    tname = get_table_name(experiment, 'Trackers', prefix)
    fname = get_output_file_name(experiment, 'Trackers')
    
    try:
        table = dynamodb.Table(tname)
        response = table.scan()
        items = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])

        res = pd.DataFrame.from_records(items)
        res = res.set_index(['ppid_session_dataname', 'trial_num']).apply(pd.Series.explode).reset_index()
        save_dataframe(res, fname)
    
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
            print('{:s}: table {:s} not found, skipping.'.format(fname, tname))


if __name__ == '__main__':

    PREFIX = 'UXFData'
    TABLES = ['ParticipantDetails', 'TrialResults', 'Settings', 'SessionLog', 'SummaryStatistics']

    ap = argparse.ArgumentParser(description='A tool to download Unity Experiment Framework data from AWS DynamoDB')
    ap.add_argument('experiment', help='Experiment name to download')
    ap.add_argument('-r', metavar='AWS_REGION', dest='region', action='store', default=None, required=True, help='AWS region to use for DynamoDB (required)')
    ap.add_argument('-p', dest='p', action='store_true', default=False, help='Only retrieve and list participant details')
    ap.add_argument('-t', dest='tracker', action='store_true', default=False, help='Also download Tracker table (caution: this might use a lot of data!)')
    ap.add_argument('-f', dest='folder', action='store_true', default=False, help='Create a subfolder using experiment name')
    ap.add_argument('--profile', dest='profile', action='store', default=None, help='Use specific profile from AWS credentials file')
    ap.add_argument('--access', dest='access', action='store', default=None, help='AWS access key to use  ')
    ap.add_argument('--secret', dest='secret', action='store', default=None, help='AWS secret key to use')
    options = ap.parse_args()

    if options.folder:
        data_folder = os.path.join(os.getcwd(), options.experiment)
        if not os.path.isdir(data_folder):
            os.mkdir(data_folder)
        prev_wd = os.getcwd()
        os.chdir(data_folder)

    # Set up session parameters and connect to DynamoDB
    sess_args = {}
    if options.region is not None:
        sess_args['region_name'] = options.region
    if options.profile is not None:
        sess_args['profile_name'] = options.profile
    if options.access is not None and options.secret is not None:
        sess_args['aws_access_key_id'] = options.access
        sess_args['aws_secret_access_key'] = options.secret
    elif options.access is not None or options.secret is not None:
        print('Error: --access and --secret must both be specified to use credentials from the command line!')
        sys.exit(-1)
    aws_session = boto3.Session(**sess_args)
    ddb = aws_session.resource('dynamodb')

    if not options.p:
        print('Retrieving results for UXF experiment "{:s}"...'.format(options.experiment))
        download_uxf_tables(ddb, options.experiment, TABLES, prefix=PREFIX)

        # Don't download tracker data by default, as this might cause high resource
        # utilization or data transfer cost in case of large datasets!
        if options.tracker:
            download_tracker_data(ddb, options.experiment, prefix=PREFIX)

        print('Download complete.')
    
    else:
        # Download and display participant details only
        print('Participant details for UXF experiment "{:s}":\n'.format(options.experiment))
        df = scan_table_to_df(ddb, get_table_name(options.experiment, 'ParticipantDetails', prefix=PREFIX), unpack=True)
        print(df)
        save_dataframe(df, get_output_file_name(options.experiment, 'ParticipantDetails'))

    if options.folder:
        os.chdir(prev_wd)