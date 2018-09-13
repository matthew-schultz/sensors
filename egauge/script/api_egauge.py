#!./env/bin/python3
"""
This script obtains egauge readings using sensor data retrieved from a database

and inserts those readings into a reading table and a success or failure timestamp
into a database_insertion_timestamp table.
"""
from orm_egauge import EgaugeORM
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import func #used for sql max() function

# import argparse
import arrow
import logging
# import os
import pandas
import requests
# import sqlalchemy #used for errors like sqlalchemy.exc.InternalError, sqlalchemy.exc.OperationalError
# import sys


class EgaugeAPI:
    FAILURE_STATUS = "FAILURE"
    SUCCESS_STATUS = "SUCCESS"
    NO_NEW_READINGS_STATUS = "NO NEW READINGS"
    API_DATA_RECEIVED_STATUS = "RECEIVED DATA FROM API"
    # SCRIPT_NAME = os.path.basename(__file__) # will be used for future process lock
    SAMPLE_RESOLUTION_IN_SECONDS = 60
    logging.basicConfig(filename='error.log', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


    # connect to database by creating a session
    def get_db_handler(self, db_url='postgresql:///sensors_project'):
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
    #     latest_datetime_successfully_inserted = conn.query(func.max(EgaugeORM.DatabaseInsertionTimestamp.timestamp)).filter_by(is_success=True)[0][0]
    #     if latest_datetime_successfully_inserted:
    #         # shift timestamp 10 hours forward since HST is GMT - 10 hours
    #         last_reading_timestamp = arrow.get(latest_datetime_successfully_inserted).shift(hours = +10)
    #     return last_reading_timestamp


    # returns a readings dataframe
    def get_data_from_api(self, conn, sensor, unit_of_time):
        # set api_end_time to current time
        api_end_time = arrow.now()
        last_reading_timestamp = ''
        # The next lines of code before setting api_start_time
        # used to be in their own function get_most_recent_timestamp_from_db()
        # conn.query(func.max...).first()[0] returns the first element in the first tuple in a list
        latest_datetime_successfully_inserted = conn.query(func.max(EgaugeORM.DatabaseInsertionTimestamp.latest_timestamp)).filter_by(status=self.SUCCESS_STATUS, sensor_id=sensor.id).first()[0]
        print("latest_datetime_successfully_inserted: ", latest_datetime_successfully_inserted)
        if latest_datetime_successfully_inserted:
            # shift timestamp 10 hours forward since HST is GMT - 10 hours
            last_reading_timestamp = arrow.get(latest_datetime_successfully_inserted).shift(hours=+10)
        # if there is no most recent timestamp in database exit script
        else:
            raise Exception("No existing latest_datetime_successfully_inserted found for ", sensor.path)
        api_start_time = last_reading_timestamp
        # add time granularity (60 seconds by default) to api_start_timestamp because we want to record last time inserted into database_insertion_timestamp and
        # the egauge api returns values inclusive of the start time and exclusive of the end time
        api_start_timestamp = api_start_time.timestamp + self.SAMPLE_RESOLUTION_IN_SECONDS
        api_end_timestamp = api_end_time.timestamp
        if api_start_timestamp > api_end_timestamp:
            raise ValueError('Error: api_start_timestamp ' + str(arrow.get(api_start_timestamp)) + ' was later than api_end_timestamp ' + str(arrow.get(api_end_timestamp)))
        output_csv = 'c'
        delta_compression = 'C'
        host = 'http://{}.egaug.es/cgi-bin/egauge-show?'
        host = host.format(str(sensor.path)) + '&' + unit_of_time + '&' + output_csv + '&' + delta_compression
        time_window = {'t': api_start_timestamp, 'f': api_end_timestamp}
        request = requests.get(host, params=time_window)
        if request.status_code == requests.codes.ok:
            print('[' + str(arrow.get(api_end_timestamp)) + '] ' + 'Request was successful' + str(request))
            readings = pandas.read_csv(StringIO(request.text))
            readings = readings.sort_values(by='Date & Time')
            # # Set header=False if we don't want to append header and set index=False to remove index column.
            # readings.to_csv(path_or_buf=output_file, index=False, header=False, mode='a+')
            # # readings.to_csv(path_or_buf=output_file, mode='a+')
            timestamp_row = EgaugeORM.DatabaseInsertionTimestamp(sensor_id=sensor.id, sensor_type=sensor.type, upload_timestamp=api_end_time.datetime, status=self.API_DATA_RECEIVED_STATUS)
            conn.add(timestamp_row)
            conn.commit()
            return readings
        else:
            request.raise_for_status()


    def insert_readings_into_database(self, conn, readings, sensor):
        current_time = arrow.now()
        # The reading time of the last row inserted into the reading table
        last_reading_row_timestamp = ''
        rows_returned = readings.shape[0]
        rows_inserted = 0
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
                    row_units = columns[i+1]  # currently inserts column names as unit
                    reading_row = EgaugeORM.Reading(sensor_id=sensor.id, timestamp=row_datetime, unit=row_units, upload_timestamp=current_time.datetime, value=row_reading)
                    conn.add(reading_row)
                    rows_inserted += 1
                    last_reading_row_timestamp = arrow.get(row_datetime)
        print(str(rows_returned) + ' row(s) returned by egauge api')
        print(str(rows_inserted) + ' readings(s) inserted by egauge api')

        if last_reading_row_timestamp:
            timestamp_row = EgaugeORM.DatabaseInsertionTimestamp(latest_timestamp=last_reading_row_timestamp.datetime, sensor_id=sensor.id, sensor_type=sensor.type, status=self.SUCCESS_STATUS, upload_timestamp=current_time.datetime)
        else:
            timestamp_row = EgaugeORM.DatabaseInsertionTimestamp(sensor_id=sensor.id, sensor_type=sensor.type, status=self.FAILURE_STATUS, upload_timestamp=current_time.datetime)
        conn.add(timestamp_row)
        conn.commit()


    def log_failure_to_connect_to_api(self, conn, sensor):
        api_call_timestamp = arrow.now()
        logging.exception('API data request error')
        timestamp_row = EgaugeORM.DatabaseInsertionTimestamp(sensor_id=sensor.id, sensor_type=sensor.type, status=self.FAILURE_STATUS, upload_timestamp=api_call_timestamp.datetime)
        conn.add(timestamp_row)
        conn.commit()


    # need to continue testing if I should call conn.rollback() in this function
    def log_failure_to_connect_to_database(self, conn, sensor):
        database_insertion_timestamp = arrow.now()
        logging.exception('Database insertion error')
        timestamp_row = EgaugeORM.DatabaseInsertionTimestamp(sensor_id=sensor.id, sensor_type=sensor.type, status=self.FAILURE_STATUS, upload_timestamp=database_insertion_timestamp.datetime)
        conn.add(timestamp_row)
        conn.commit()


    # need first timestamp already in database insertion timestamp for script to run successfully
    def run(self):
        """
        This code block attempts to pull egauge sensor readings and insert them into a database.

        It inserts a row representing the success or failure of the api request attempt and a row the success or
        failure of the reading insertion attempt into the database_insertion_timestamp table.

        Data pulled will start at 60 seconds past the time read from the database_insertion_timestamp
        up to (but not including) the current time.

        Any exceptions thrown during execution will be logged to a file.

        Possible future arguments:
            project_database_path: a string used to connect to a specific project's database
        """

        unit_of_time = 'm'
        # start the database connection
        conn = self.get_db_handler()
        sensors = conn.query(EgaugeORM.Sensor).filter_by(type="EGAUGE")
        for sensor in sensors:
            try:
                # readings is a pandas dataframe
                readings = self.get_data_from_api(conn, sensor, unit_of_time)
            # catch egauge api request exceptions like requests.exceptions.ConnectionError, ValueError
            except (requests.exceptions.ConnectionError, Exception) as e:
                self.log_failure_to_connect_to_api(conn, sensor)
                continue
            try:
                self.insert_readings_into_database(conn, readings, sensor)
            # catch database errors like sqlalchemy.exc.InternalError, sqlalchemy.exc.OperationalError
            except Exception as e:
                self.log_failure_to_connect_to_database(conn, sensor)
        conn.close()
