#!../../egauge/script/env/bin/python3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import csv
import glob
import logging
import orm_hobo
import os
import pandas
import pendulum


logging.basicConfig(filename='error.log',format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
#insertion statuses
FAILURE_STATUS = "FAILURE"
SUCCESS_STATUS = "SUCCESS"
NO_NEW_READINGS_STATUS = "NO NEW READINGS"


# connect to database by creating a session
def get_db_handler(db_url='postgresql:///hobo'):
    # connect to the database
    db = create_engine(db_url)
    Session = sessionmaker(db)
    conn = Session()
    return conn

"""
Create reading dataframe and metadata list using csv file

Takes database session 'conn' and 'csv_filename' string as arguments

Opens csv file, reads file as dataframe and extracts metadata into a list
Checks if the timestamp of the earliest and latest rows in dataframe are already in db for a given sensor_id
"""
def get_csv_from_folder_not_in_db(conn, csv_filename):
    #assume there are no new readings by default
    no_new_readings = True
    with open(csv_filename, 'r') as file:
        reader = csv.reader(file)
        line1 = next(reader) # Remove the first row, which breaks the csv format and contains the hobo sensor id
        line1 = line1[0]
        #Extract sensor_id
        # sensor_id = line1.split(': ')[1][0:-1] #this commented out line has a weird bug that sometimes removes the last digit instead of the trailing quotation mark
        sensor_id = line1.split(': ')[1][0:]
        if sensor_id[-1:] is "\"":
            sensor_id = sensor_id[0:-1]
        #Store in table the remainder of the table
        table = list(reader)
    # #TEST
    # print(table[0:3])
    csv_readings = pandas.DataFrame(table[1:],columns=table[0])
    csv_readings = csv_readings.iloc[:,1:]
    #Extract timezone and units
    timezone_units = csv_readings.columns
    #First separate the variable name from the timezone/unit description
    names = [x.split(', ')[0] for x in timezone_units]
    csv_readings.columns = names
    # Units
    timezone_units = [x.split(', ')[1] for x in timezone_units]
    # #Timezone needs no further pre-processing
    # timezone = timezone_units[0]
    #But units do:
    units = [x.split(' ')[0] for x in timezone_units[1:]]
    #Remove duplicates
    csv_readings = csv_readings.drop_duplicates(subset='Date Time')
    #convert date column from string objects to datetimes
    csv_readings['Date Time'] = pandas.to_datetime(csv_readings['Date Time'])
    #sort csv_readings dataframe by timestamp
    csv_readings = csv_readings.sort_values(by=['Date Time'])
    # #TEST
    # csv_readings.to_csv(path_or_buf='output.txt')
    csv_modified_timestamp = pendulum.from_timestamp(os.path.getmtime(csv_filename), tz='Pacific/Honolulu')
    #get earliest_csv_timestamp
    earliest_csv_timestamp = pendulum.instance(csv_readings.iloc[0]['Date Time'], 'Pacific/Honolulu')
    #get latest_csv_timestamp
    latest_csv_timestamp = pendulum.instance(csv_readings.iloc[csv_readings.shape[0]-1]['Date Time'], 'Pacific/Honolulu')
    # check if earliest and latest file_timestamps are already in db and set no_new_readings variable
    # assume that if first or last timestamps in csv were already inserted for that given timestamp and sensor_id, then all are
    earliest_csv_timestamp_in_db = conn.query(orm_hobo.Reading).filter_by(timestamp=earliest_csv_timestamp, sensor_id=sensor_id).first()
    latest_csv_timestamp_in_db = conn.query(orm_hobo.Reading).filter_by(timestamp=latest_csv_timestamp, sensor_id=sensor_id).first()
    if not earliest_csv_timestamp_in_db or not latest_csv_timestamp:
        no_new_readings = False
    return csv_readings, (no_new_readings, earliest_csv_timestamp, csv_modified_timestamp, sensor_id, latest_csv_timestamp, units)


def insert_csv_readings_into_db(conn, csv_readings, csv_metadata, csv_filename):
    current_timestamp = pendulum.now('Pacific/Honolulu')
    #useful if main does not use continue
    #check if csv_readings was initialized as a dataframe
    if isinstance(csv_readings, pandas.DataFrame):
        if csv_readings.empty:
            print('could not extract readings from csv')
            return
        else:
            print('readings extracted from csv')
    #executes if not initialized as a dataframe
    elif not csv_readings:
        print('csv_readings set to None')
        return
    no_new_readings, earliest_csv_timestamp, csv_modified_timestamp, sensor_id, latest_csv_timestamp, units = csv_metadata
    if no_new_readings:
        raise Exception("csv readings already inserted")
    status = FAILURE_STATUS
    rows_returned = csv_readings.shape[0]
    if rows_returned > 0:
        #Format the temperature sensor table and insert
        temperature_sensor = csv_readings.loc[:,['Date Time','Temp']]
        for temperature_row in temperature_sensor.itertuples():
            #The next two commented lines are for reference when setting timestamps using pendulum
            # temperature_row_timestamp = pendulum.from_format(temperature_row[1], 'MM/DD/YY hh:mm:ss A', 'Pacific/Honolulu')
            # temperature_row_timestamp = pendulum.instance(temperature_row[1], 'Pacific/Honolulu')
            reading_temperature_row = orm_hobo.Reading(sensor_id=sensor_id, timestamp=temperature_row[1], reading=temperature_row[2], units=units[0] + ' ' + temperature_sensor.columns.values[1], upload_timestamp=current_timestamp)
            conn.add(reading_temperature_row)
        #Format the humidity sensor table and insert
        humidity_sensor = csv_readings.loc[:,['Date Time','RH']]
        for humidity_row in humidity_sensor.itertuples():
            reading_humidity_row = orm_hobo.Reading(sensor_id=sensor_id, timestamp=humidity_row[1], reading=humidity_row[2], units=str(units[1] + ' ' + humidity_sensor.columns.values[1]), upload_timestamp=current_timestamp)
            conn.add(reading_humidity_row)
        #Format the intensity sensor table and insert
        intensity_sensor = csv_readings.loc[:,['Date Time','Intensity']]
        for intensity_row in intensity_sensor.itertuples():
            reading_intensity_row = orm_hobo.Reading(sensor_id=sensor_id, timestamp=intensity_row[1], reading=intensity_row[2], units=str(units[2] + ' ' + intensity_sensor.columns.values[1]), upload_timestamp=current_timestamp)
            conn.add(reading_intensity_row)
        status = SUCCESS_STATUS
    timestamp_row = orm_hobo.DatabaseInsertionTimestamp(status=status, sensor_id=sensor_id, upload_timestamp=current_timestamp, earliest_csv_timestamp=earliest_csv_timestamp, csv_modified_timestamp=csv_modified_timestamp, latest_csv_timestamp=latest_csv_timestamp, csv_filename=csv_filename)
    conn.add(timestamp_row)
    conn.commit()


def log_failure_to_get_csv_readings_from_folder_not_in_db(conn, csv_filename):
    current_timestamp = pendulum.now('Pacific/Honolulu')
    logging.exception('log_failure_to_get_csv_readings_from_folder_not_in_db')
    timestamp_row = orm_hobo.DatabaseInsertionTimestamp(csv_filename=csv_filename, status=FAILURE_STATUS, upload_timestamp=current_timestamp)
    conn.add(timestamp_row)
    conn.commit()


def log_failure_to_insert_csv_readings_into_db(conn, csv_filename, csv_metadata):
    current_timestamp = pendulum.now('Pacific/Honolulu')
    no_new_readings, earliest_csv_timestamp, csv_modified_timestamp, sensor_id, latest_csv_timestamp, units = csv_metadata
    if no_new_readings:
        status=NO_NEW_READINGS_STATUS
    else:
        #log error in file only for failure status
        logging.exception('log_failure_to_insert_csv_readings_into_db')
        status=FAILURE_STATUS
    timestamp_row = orm_hobo.DatabaseInsertionTimestamp(earliest_csv_timestamp=earliest_csv_timestamp, csv_filename=csv_filename, csv_modified_timestamp=csv_modified_timestamp, sensor_id=sensor_id, status=status, latest_csv_timestamp=latest_csv_timestamp, upload_timestamp=current_timestamp)
    #rollback any reading insertions during that iteration of for loop in main
    conn.rollback()
    conn.add(timestamp_row)
    conn.commit()


if __name__=='__main__':
    conn = get_db_handler()
    csv_filenames = glob.glob('./to-insert/*.csv')
    for csv_filename in csv_filenames:
        try:
             csv_readings, csv_metadata = get_csv_from_folder_not_in_db(conn, csv_filename)
        except Exception as e:
             log_failure_to_get_csv_readings_from_folder_not_in_db(conn, csv_filename)
             continue
        try:
             insert_csv_readings_into_db(conn, csv_readings, csv_metadata, csv_filename)
        except Exception as e:
             log_failure_to_insert_csv_readings_into_db(conn, csv_filename, csv_metadata)
