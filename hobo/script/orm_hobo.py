"""
This module defines classes for the postgresql tables that store database insertion success and readings
and functions relating to those tables
"""

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String #, Boolean
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import csv


#needs to be global so ORM classes can be initialized
BASE = declarative_base()
DB_URL = 'postgresql:///hobo'


class DatabaseInsertionTimestamp(BASE):
    """
    This class represents the database_insertion_timestamp table

    Columns:
        id: uniquely identifies a row
        sensor_id: uniquely identifies a hobo sensor
        csv_filename: csv file
        csv_modified_timestamp: when the csv file was last modified
        status: represents if the reading insertion was a "SUCCESS" or "FAILURE", or if "NO NEW DATA" was read
        upload_timestamp: when a csv file was read or a reading insertion was attempted
        earliest_csv_timestamp: earliest timestamp in the csv
        latest_csv_timestamp: latest timestamp in the csv
    """
    __tablename__ = 'database_insertion_timestamp'

    # the sqlalchemy orm requires a primary key in each table
    id = Column(Integer, primary_key=True)
    sensor_id = Column(String)
    csv_filename = Column(String)
    csv_modified_timestamp = Column(TIMESTAMP)
    status = Column(String)
    upload_timestamp = Column(TIMESTAMP) # timestamp = Column(TIMESTAMP)
    earliest_csv_timestamp = Column(TIMESTAMP)
    latest_csv_timestamp = Column(TIMESTAMP)


class Reading(BASE):
    """
    This class represents the reading table

    The table contains data read by the sensor for given units of time (usually minutes)

    Columns:
        reading_id: uniquely identifies a reading
        sensor_id: a string representing the hobo id
        timestamp: the reading's timestamp
        units: corresponds to the column name of the reading
        reading: the numerical value of a reading
        upload_timestamp: the time when the readings in one file were inserted
    """
    __tablename__ = 'reading'

    reading_id = Column(Integer, primary_key=True)
    sensor_id = Column(String)
    timestamp = Column(TIMESTAMP)
    units = Column(String)
    reading = Column(DOUBLE_PRECISION)
    upload_timestamp = Column(TIMESTAMP)


def setup(db_url=DB_URL):
    """
    Setup tables for testing in a given database
    """
    db = create_engine(db_url)
    Session = sessionmaker(db)
    session = Session()
    BASE.metadata.create_all(db)
    session.commit()
    session.close()


def teardown(db_url=DB_URL):
    """
    Drop each table that was set up for testing in a given database
    """
    db = create_engine(db_url)
    Reading.__table__.drop(db)
    DatabaseInsertionTimestamp.__table__.drop(db)


def export_reading_to_csv(db_url=DB_URL, output_filename='reading_dump.csv'):
    db = create_engine(db_url)
    Session = sessionmaker(db)
    session = Session()

    with open(output_filename, 'w') as outfile:
        outcsv = csv.writer(outfile)
        rows = session.query(Reading)
        for row in rows:
            outcsv.writerow([row.reading_id, row.sensor_id, row.timestamp, row.units, row.reading])
    session.close()
