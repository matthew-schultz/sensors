#!./env/bin/python3
"""
This module defines classes for the postgresql tables that store database insertion success and readings
and functions relating to those tables
"""
from pathlib import Path #used to read db_url_config.txt in parent directory
from sqlalchemy import create_engine
from sqlalchemy import Boolean, Column, Integer, String, Enum
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.schema import ForeignKey

# import csv
# import enum
import os


# needs to be in the same scope as all ORM table classes because they are subclasses of declarative_base class
BASE = declarative_base()
DB_URL = 'postgresql:///sensors'


class ErrorStatus:
    """
    This class defines strings that could be inserted into error_log.error_status

    Each string represents the status of the api script execution when an error_log row was inserted
        failure: failure to obtain readings from source or insert new readings
        no_new_readings: successfully obtained readings from purpose but they were already in database
        success: successfully obtained readings from purpose and inserted them into readings table
    """
    failure = "failure"
    no_new_readings = "no_new_readings"
    success = "success"


class PipelineStage:
    """
    This class defines strings that could be inserted into error_log.pipeline_stage

    Each string represents at what stage of the api script execution an error_log row was inserted
        data_acquisition: attempting to obtain readings from source
        database_insertion: attempting to insert new rows into readings table
    """
    data_acquisition = "data_acquisition"
    database_insertion = "database_insertion"


class Project(BASE):
    """
    This class represents the project table

    Columns:
        project_folder_path: the full path of the folder where a project's files and folders are stored
    """
    __tablename__ = 'project'

    project_folder_path = Column(String, primary_key=True)


class ErrorLog(BASE):
    """
    This class represents the error_log table

    Columns:
        id: uniquely identifies a row
        sensor_id: foreign key to sensor table
        latest_timestamp: the latest reading timestamp of a successful insertion attempt
        status: represents if the request or insertion was successful
        datetime: when an api request was called or a reading insertion was attempted
    """
    __tablename__ = 'error_log'

    # the sqlalchemy orm requires a primary key in each table
    log_id = Column(Integer, primary_key=True)
    purpose_id = Column(Integer)
    sensor_id = Column(String)
    datetime = Column(TIMESTAMP)
    status = Column(String)
    # error_type = Column(String)
    pipeline_stage = Column(String)

    # def __repr__(self):
    #     return "<ErrorLog(id='%s', timestamp='%s, is_success='%s')>" % (
    #                          self.id, self.timestamp, self.is_success)


class Readings(BASE):
    """
    This class represents the reading table

    The table contains data read by the sensor for given units of time (usually minutes)

    Columns:
        datetime: the reading's datetime
        purpose_id: unique id representing a purpose
        value: the numerical value of a reading
    """
    __tablename__ = 'readings'

    datetime = Column(TIMESTAMP, primary_key=True)
    purpose_id = Column(Integer, primary_key=True)
    value = Column(DOUBLE_PRECISION)


class SensorInfo(BASE):
    """
    Sources of readings

    Columns:
        purpose_id: integer uniquely identifying a purpose
        sensor_id: string uniquely id'ing a sensor; used in egauge and webctrl API requests; hobo sensor serial number
            one sensor_id may have multiple purposes (egauge)
        sensor_part: string that represents one column name in data from a sensor if one row of data has multiple readings
        sensor_type: string representing how readings are accessed; e.g. egauge, webctrl, hobo
        is_active: boolean representing if script should request data from a sensor
        last_updated_datetime: postgres timestamp used to keep track of datetime of last successfully inserted reading
    """
    __tablename__ = 'sensor_info'

    purpose_id = Column(Integer, primary_key=True)
    sensor_id = Column(String)
    sensor_part = Column(String)
    sensor_type = Column(String)
    is_active = Column(Boolean)
    last_updated_datetime = Column(TIMESTAMP)


class ApiAuthentication(BASE):
    """
    User info for authentication

    Currently used to connect to webctrl api
    """
    __tablename__ = 'api_authentication'

    user_id = Column(Integer, primary_key=True)
    username = Column(String)
    password = Column(String)


class ErrorLogDetails(BASE):
    """
    This class represents the error_log_details table that houses any extra info needed to troubleshoot script problems.

    error_log_details is a long-form table.
    It is currently used with hobo scripts to store filename, first and last reading timestamps,
    as one hobo has multiple files, with potentially repeated names.
    """
    __tablename__ = 'error_log_details'

    log_id = Column(Integer, primary_key=True)
    information_type = Column(String, primary_key=True)
    information_value = Column(String)


def setup():
    """
    Setup tables for testing in a given database
    """
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    # db_url_config = str(Path(dir_path).parent.parent) + "/db_url_config.txt"
    # with open(db_url_config, "r") as db_url_config_file:
    #     db_url = db_url_config_file.read()
    db = create_engine(DB_URL)
    BASE.metadata.create_all(db)


# def teardown(, db_url=DB_URL):
def teardown():
    """
    Drop each table that was set up for testing in a given database
    """
    db = create_engine(DB_URL)
    BASE.metadata.drop_all(db)
    # Readings.__table__.drop(db) # how to drop one table

# def export_reading_to_csv(db_url=DB_URL, output_filename='reading_dump.csv'):
#     db = create_engine(db_url)
#     Session = sessionmaker(db)
#     session = Session()
#
#     with open(output_filename, 'w') as outfile:
#         outcsv = csv.writer(outfile)
#         rows = session.query(Reading)
#         for row in rows:
#             outcsv.writerow([row.reading_id, row.sensor_id, row.timestamp, row.units, row.reading])
#     session.close()
