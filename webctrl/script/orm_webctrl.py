"""
This module assists with managing the webctrl sqlalchemy orm.

"""

from sqlalchemy import create_engine
from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.dialects.postgresql import TIMESTAMP #, DOUBLE_PRECISION
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import func

import csv

#needs to be global so ORM classes can be initialized
BASE = declarative_base()
DB_URL = 'postgresql:///webctrl'

class DatabaseInsertionTimestamp(BASE):
    """
    This class represents the database_insertion_timestamp table

    Columns:
        id: uniquely identifies a row
        timestamp: the last time an api request was called
        is_success: represents if the request was successful
    """

    __tablename__ = 'database_insertion_timestamp'

    # the sqlalchemy orm requires a primary key in each table
    id = Column(Integer, primary_key=True)
    timestamp = Column(TIMESTAMP)
    is_success = Column(Boolean)

    # def __repr__(self):
    #     return "<DatabaseInsertionTimestamp(id='%s', timestamp='%s, is_success='%s')>" % (
    #                          self.id, self.timestamp, self.is_success)

class Reading(BASE):
    """
    This class represents the database_insertion_timestamp table

    The table contains data read by the sensor for given units of time (usually minutes)

    Columns:
        reading_id: uniquely identifies a reading
        sensor_path: the path of the webctrl sensor that made the reading
        timestamp: the reading's timestamp
        units: corresponds to the datatype of the reading (a, d, or b)
        as obtained in its webctrl api request
        reading: the value of a reading
        upload_timestamp: the time when the api request was made;
        floored to the nearest minute
    """

    __tablename__ = 'reading'

    reading_id = Column(Integer, primary_key=True)
    sensor_path = Column(String)
    timestamp = Column(TIMESTAMP)
    units = Column(String)
    reading = Column(String)
    upload_timestamp = Column(TIMESTAMP)

class WebctrlUser(BASE):
    """
    User info for authentication into webctrl api

    Columns:
        user_id
        username
        password
    """

    __tablename__ = 'webctrl_user'

    user_id = Column(Integer, primary_key=True)
    username = Column(String)
    password = Column(String)

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
    DatabaseInsertionTimestamp.__table__.drop(db)
    Reading.__table__.drop(db)

def export_to_csv(db_url=DB_URL, table=Reading, output_filename='reading_dump.csv'):
    db = create_engine(db_url)
    Session = sessionmaker(db)
    session = Session()

    with open(output_filename, 'w') as outfile:
        outcsv = csv.writer(outfile)
        rows = session.query(table)
        for row in rows:
            outcsv.writerow([row.reading_id, row.sensor_path, row.timestamp, row.units, row.reading])
    session.close()
