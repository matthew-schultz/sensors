#!./env/bin/python3
"""
This script makes egauge api requests using data retrieved from a database

and inserts that data into a reading table and a success or failure timestamp
into a database_insertion_timestamp table.
"""

from io import StringIO

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import func #used for sql max() function

# import argparse
import arrow
import logging
import orm_egauge
# import os
import pandas
import requests
# import sqlalchemy #used for errors like sqlalchemy.exc.InternalError, sqlalchemy.exc.OperationalError
import sys


#SCRIPT_NAME = os.path.basename(__file__) # will be used for future process lock
TIME_GRANULARITY_IN_SECONDS = 60
logging.basicConfig(filename='error.log',format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


# connect to database by creating a session
def get_db_handler(db_url='postgresql:///egauge'):
    # connect to the database
    db = create_engine(db_url)
    Session = sessionmaker(db)
    conn = Session()
    return conn


#THIS FUNCTION WAS DISSOLVED INTO get_data_from_api() BUT REMAINS HERE FOR REFERENCE
# # will need to update once relating each database_insertion_time to egauge sensor is decided (add filter for sensor id)
# # Obtains the latest 'success' timestamp in database_insertion_timestamp
# # which should be the the same timestamp for the last reading successfully inserted into the reading table
# def get_most_recent_timestamp_from_db(conn):
#     last_reading_timestamp = ''
#     # conn.query(func.max...)[0][0] returns the first element in the first tuple in a list
#     latest_datetime_successfully_inserted = conn.query(func.max(orm_egauge.DatabaseInsertionTimestamp.timestamp)).filter_by(is_success=True)[0][0]
#     if latest_datetime_successfully_inserted:
#         # shift timestamp 10 hours forward since HST is GMT - 10 hours
#         last_reading_timestamp = arrow.get(latest_datetime_successfully_inserted).shift(hours = +10)
#     return last_reading_timestamp


# returns a readings dataframe
def get_data_from_api(conn, sensor_id, unit_of_time):
    # set api_end_time to current time
    api_end_time = arrow.now()
    last_reading_timestamp = ''
    # The next lines of code before setting api_start_time
    # used to be in their own function get_most_recent_timestamp_from_db()
    # conn.query(func.max...)[0][0] returns the first element in the first tuple in a list
    latest_datetime_successfully_inserted = conn.query(func.max(orm_egauge.DatabaseInsertionTimestamp.timestamp)).filter_by(is_success=True)[0][0]
    if latest_datetime_successfully_inserted:
        # shift timestamp 10 hours forward since HST is GMT - 10 hours
        last_reading_timestamp = arrow.get(latest_datetime_successfully_inserted).shift(hours = +10)
    # if there is no most recent timestamp in database exit script
    else:
        sys.exit()
    api_start_time = last_reading_timestamp
    # add time granularity (60 seconds by default) to api_start_timestamp because we want to record last time inserted into database_insertion_timestamp and
    # the egauge api returns values inclusive of the start time and exclusive of the end time
    api_start_timestamp = api_start_time.timestamp + TIME_GRANULARITY_IN_SECONDS
    api_end_timestamp = api_end_time.timestamp
    if api_start_timestamp > api_end_timestamp:
        raise ValueError('Error: api_start_timestamp ' + str(arrow.get(api_start_timestamp)) + ' was later than api_end_timestamp ' + str(arrow.get(api_end_timestamp)))
    output_csv = 'c'
    delta_compression = 'C'
    host = 'http://{}.egaug.es/cgi-bin/egauge-show?'
    host = host.format(str(sensor_id)) + '&' + unit_of_time + '&' + output_csv + '&' + delta_compression
    time_window = {'t': api_start_timestamp, 'f': api_end_timestamp}
    request = requests.get(host, params=time_window)
    if request.status_code == requests.codes.ok:
        print('[' + str(arrow.get(api_end_timestamp)) + '] ' + 'Request was successful' + str(request))
        readings = pandas.read_csv(StringIO(request.text))
        readings = readings.sort_values(by='Date & Time')
        # # Set header=False if we don't want to append header and set index=False to remove index column.
        # readings.to_csv(path_or_buf=output_file, index=False, header=False, mode='a+')
        # # readings.to_csv(path_or_buf=output_file, mode='a+')
        timestamp_row = orm_egauge.DatabaseInsertionTimestamp(timestamp=api_end_time.datetime)
        conn.add(timestamp_row)
        conn.commit()
        return readings
    else:
        request.raise_for_status()
    conn.close()


def insert_readings_into_database(conn, readings, sensor_id):
    current_time = arrow.now()
    # The reading time of the last row inserted into the reading table
    last_reading_row_timestamp = ''
    rows_returned = readings.shape[0]
    # check if any values were returned
    if rows_returned > 0:
        # get a list of column names from readings dataframe
        columns = list(readings.columns.values)
        # attempt to insert data from each row of the readings dataframe into reading table
        for row in readings.itertuples():
            row_datetime = arrow.get(row[1]).datetime
            for i, column_reading in enumerate(row[2:]):
                #TEST
                # print('i: ', i, ', column_reading: ',column_reading, 'columns[i+1]: ', columns[i+1])
                row_reading = column_reading
                row_units = columns[i+1] # currently inserts column names as unit
                # the upload timestamp for all rows in the readings dataframe will use the same value (current_time)
                reading_row = orm_egauge.Reading(sensor_id=sensor_id, timestamp=row_datetime, units=row_units, reading=row_reading, upload_timestamp=current_time.datetime)
                conn.add(reading_row)
                last_reading_row_timestamp = arrow.get(row_datetime)
    print(str(rows_returned) + ' row(s) returned by egauge api')

    if last_reading_row_timestamp:
        timestamp_row = orm_egauge.DatabaseInsertionTimestamp(timestamp=last_reading_row_timestamp.datetime, is_success=True)
    else:
        timestamp_row = orm_egauge.DatabaseInsertionTimestamp(timestamp=current_time.datetime, is_success=False)
    conn.add(timestamp_row)
    conn.commit()
    conn.close()


def log_failure_to_connect_to_api(conn):
    api_call_timestamp = arrow.now()
    logging.exception('API data request error')
    timestamp_row = orm_egauge.DatabaseInsertionTimestamp(timestamp=api_call_timestamp.datetime, is_success=False)
    conn.add(timestamp_row)
    conn.commit()
    conn.close()


# need to continue testing if I should call conn.rollback() in this function
def log_failure_to_connect_to_database(conn):
    database_insertion_timestamp = arrow.now()
    logging.exception('Database insertion error')
    timestamp_row = orm_egauge.DatabaseInsertionTimestamp(timestamp=database_insertion_timestamp.datetime, is_success=False)
    conn.add(timestamp_row)
    conn.commit()
    conn.close()


# need first timestamp already in database insertion timestamp for script to run successfully
if __name__ == "__main__":
    """
    This code block attempts to pull egauge sensor readings and insert them into a database.

    Afterwards, a row representing the success or failure of the attempt will be inserted into
    the database_insertion_timestamp table.

    Data pulled will start at 60 seconds past the time read from the database_insertion_timestamp
    up to (but not including) the current time.

    Any exceptions thrown during execution will be logged to a file.

    Possible future arguments:
        sensor_id: a string representing the id of the egauge sensor
        unit_of_time: the unit of time we want each reading returned over (per minute, per hour, etc)
    """
    sensor_id = 'egauge31871'
    unit_of_time = 'm'
    # start the database connection
    conn = get_db_handler()
    try:
        # readings is a pandas dataframe
        readings = get_data_from_api(conn, sensor_id, unit_of_time)
    # catch egauge api request exceptions like requests.exceptions.ConnectionError, ValueError
    except (requests.exceptions.ConnectionError, Exception) as e:
        log_failure_to_connect_to_api(conn)
        sys.exit()
    try:
        insert_readings_into_database(conn, readings, sensor_id)
    # catch database errors like sqlalchemy.exc.InternalError, sqlalchemy.exc.OperationalError
    except Exception as e:
        log_failure_to_connect_to_database(conn)
        sys.exit()
