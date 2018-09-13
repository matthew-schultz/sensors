#!./env/bin/python3
"""
This script obtains egauge readings using sensor data retrieved from a database

and inserts those readings into a readings table and a success or failure timestamp
into an error_log table.

The last_updated_datetime in the sensor_info table should be set to a valid value for script to run successfully
"""
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import logging
import orm_egauge
# import os
import pandas
import pendulum
import requests
# import sqlalchemy #used for errors like sqlalchemy.exc.InternalError, sqlalchemy.exc.OperationalError
# import sys


SAMPLING_RATE_IN_SECONDS = 60
# SCRIPT_NAME = os.path.basename(__file__) # will be used for future process lock
logging.basicConfig(filename='error.log', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


# connect to database by creating a session
def get_db_handler(db_url='postgresql:///sensors'):
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
#     latest_datetime_successfully_inserted = conn.query(func.max(orm_egauge.ErrorLog.timestamp)).filter_by(is_success=True)[0][0]
#     if latest_datetime_successfully_inserted:
#         # shift timestamp 10 hours forward since HST is GMT - 10 hours
#         last_reading_timestamp = arrow.get(latest_datetime_successfully_inserted).shift(hours = +10)
#     return last_reading_timestamp


# returns a readings dataframe
def get_data_from_api(conn, sensor_id):
    current_time = pendulum.now('Pacific/Honolulu')
    # The next lines of code before setting api_start_time used to be in their own function get_most_recent_timestamp_from_db()
    # latest_datetime_successfully_inserted = conn.query(func.max(orm_egauge.ErrorLog.latest_timestamp)).filter_by(is_success=True, sensor_id=sensor.sensor_id).first()[0]
    # last_updated_datetime = pendulum.instance(conn.query(orm_egauge.SensorInfo.last_updated_datetime).filter_by(purpose_id=sensor.purpose_id).first()[0])
    sensors = conn.query(orm_egauge.SensorInfo.purpose_id, orm_egauge.SensorInfo.sensor_part, orm_egauge.SensorInfo.last_updated_datetime).\
        filter_by(sensor_id=sensor_id, is_active=True)
    datetimes = []
    for purpose_sensor in sensors:
        datetimes.append(purpose_sensor.last_updated_datetime)
    datetimes.sort()
    last_updated_datetime = datetimes[0]
    print('\nlast_updated_datetime: ', last_updated_datetime)
    if last_updated_datetime:
        # Adjust time since egauge api uses GMT, and GMT is HST + 10 hours. Add SAMPLING_RATE... (60 seconds by default)
        # to api_start_timestamp b/c the egauge api returns values including the start time and excluding the end time.
        api_start_timestamp = pendulum.instance(last_updated_datetime).add(hours=10).add(seconds=SAMPLING_RATE_IN_SECONDS).int_timestamp
    # if there is no most recent timestamp in database, return exception
    else:
        raise Exception('No existing last_updated_datetime found for ', sensor_id)
    current_timestamp = current_time.int_timestamp
    if api_start_timestamp > current_timestamp:
        raise ValueError('Error: api_start_timestamp ' + str(api_start_timestamp) + ' was later than current_timestamp ' + str(current_timestamp))
    delta_compression = 'C'
    output_csv = 'c'
    unit_of_time = 'm'
    host = 'http://{}.egaug.es/cgi-bin/egauge-show?'
    host = host.format(str(sensor_id)) + '&' + unit_of_time + '&' + output_csv + '&' + delta_compression
    time_window = {'t': api_start_timestamp, 'f': current_timestamp}
    request = requests.get(host, params=time_window)
    if request.status_code == requests.codes.ok:
        print('[' + str(current_timestamp) + '] ' + 'Request was successful' + str(request))
        readings = pandas.read_csv(StringIO(request.text))
        readings = readings.sort_values(by='Date & Time')
        # # Set header=False if we don't want to append header and set index=False to remove index column.
        # readings.to_csv(path_or_buf=output_file, index=False, header=False, mode='a+')
        # # readings.to_csv(path_or_buf=output_file, mode='a+')
        for purpose_sensor in sensors:
            error_log_row = orm_egauge.ErrorLog(purpose_id=purpose_sensor.purpose_id, sensor_id=sensor_id, datetime=current_time, status=orm_egauge.ErrorStatus.success, pipeline_stage=orm_egauge.PipelineStage.data_acquisition)
            conn.add(error_log_row)
            conn.commit()
        return readings, sensors
    else:
        request.raise_for_status()


def insert_readings_into_database(conn, readings, sensor_id, sensors):
    current_time = pendulum.now('Pacific/Honolulu')
    for sensor in sensors:
        # The reading time of the last row inserted into the reading table
        last_reading_row_datetime = ''
        rows_returned = readings.shape[0]
        rows_inserted = 0
        # check if any values were returned
        if rows_returned > 0:
            # get a list of column names from readings dataframe
            columns = list(readings.columns.values)
            # attempt to insert data from each row of the readings dataframe into reading table
            for row in readings.itertuples():
                # appears that no timezone shifting needed but needs further testing
                row_datetime = pendulum.from_timestamp(row[1]) #.in_timezone('Pacific/Honolulu')
                for i, column_reading in enumerate(row[2:]):
                    #TEST
                    # print('i: ', i, ', column_reading: ',column_reading, 'columns[i+1]: ', columns[i+1])
                    row_reading = column_reading
                    row_sensor_part = columns[i+1]  # currently inserts column names as sensor_part
                    if sensor.sensor_part == row_sensor_part and pendulum.instance(sensor.last_updated_datetime) < row_datetime.subtract(hours=10):
                        # purpose_ids.append(sensor.purpose_id)
                        reading_row = orm_egauge.Readings(purpose_id=sensor.purpose_id, datetime=row_datetime, value=row_reading)
                        conn.add(reading_row)
                        rows_inserted += 1
                        last_reading_row_datetime = row_datetime
        print(str(rows_returned) + ' row(s) returned by egauge api')
        print(str(rows_inserted) + ' readings(s) inserted by egauge api')
        if rows_inserted > 0:
            status = orm_egauge.ErrorStatus.success
            conn.query(orm_egauge.SensorInfo.purpose_id).filter(orm_egauge.SensorInfo.purpose_id == sensor.purpose_id).update(
                {"last_updated_datetime": last_reading_row_datetime})
        else:
            status = orm_egauge.ErrorStatus.no_new_readings
        error_log_row = orm_egauge.ErrorLog(purpose_id=sensor.purpose_id, sensor_id=sensor_id, datetime=current_time, pipeline_stage=orm_egauge.PipelineStage.database_insertion, status=status)
        conn.add(error_log_row)
        conn.commit()


def log_failure_to_connect_to_api(conn, sensor_id):
    current_time = pendulum.now('Pacific/Honolulu')
    logging.exception('API data request error')
    error_log_row = orm_egauge.ErrorLog(sensor_id=sensor_id, datetime=current_time, pipeline_stage=orm_egauge.PipelineStage.data_acquisition, status=orm_egauge.ErrorStatus.failure)
    conn.add(error_log_row)
    conn.commit()


# need to continue testing if I should call conn.rollback() in this function
def log_failure_to_connect_to_database(conn, sensor_id):
    current_time = pendulum.now('Pacific/Honolulu')
    logging.exception('Database insertion error')
    conn.rollback()
    error_log_row = orm_egauge.ErrorLog(sensor_id=sensor_id, datetime=current_time, pipeline_stage=orm_egauge.PipelineStage.database_insertion, status=orm_egauge.ErrorStatus.failure)
    conn.add(error_log_row)
    conn.commit()


if __name__ == '__main__':
    # start the database connection
    conn = get_db_handler()
    sensor_ids = [sensor_id[0] for sensor_id in conn.query(orm_egauge.SensorInfo.sensor_id).filter_by(sensor_type='egauge', is_active=True).distinct()]
    for sensor_id in sensor_ids:
        try:
            # readings is a pandas dataframe
            readings, sensors = get_data_from_api(conn, sensor_id)
        # catch egauge api request exceptions like requests.exceptions.ConnectionError, ValueError
        except (requests.exceptions.ConnectionError, Exception) as e:
            log_failure_to_connect_to_api(conn, sensor_id)
            continue
        try:
            insert_readings_into_database(conn, readings, sensor_id, sensors)
        # catch database errors like sqlalchemy.exc.InternalError, sqlalchemy.exc.OperationalError
        except Exception as e:
            log_failure_to_connect_to_database(conn, sensor_id)
    conn.close()
