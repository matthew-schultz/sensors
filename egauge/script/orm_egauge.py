"""
This module defines classes for the postgresql tables that store database insertion success and readings
and functions relating to those tables
"""

from sqlalchemy import create_engine
from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# import csv


class EgaugeORM:
    # needs to be in the same scope as all ORM table classes so they can be initialized
    BASE = declarative_base()
    DB_URL = 'postgresql:///sensors_project'


    class DatabaseInsertionTimestamp(BASE):
        """
        This class represents the database_insertion_timestamp table

        Columns:
            id: uniquely identifies a row
            sensor_id: foreign key to sensor table
            latest_timestamp: the latest reading timestamp of a successful insertion attempt
            sensor_type: a string in ALL CAPS representing the source of the data
            status: represents if the request or insertion was successful
            upload_timestamp: when an api request was called or a reading insertion was attempted
        """
        __tablename__ = 'database_insertion_timestamp'

        # fields order: ids, shared fields, exclusive fields, alphabetical
        # the sqlalchemy orm requires a primary key in each table
        id = Column(Integer, primary_key=True)
        sensor_id = Column(Integer)
        latest_timestamp = Column(TIMESTAMP)
        sensor_type = Column(String)
        status = Column(String)
        upload_timestamp = Column(TIMESTAMP)
        # csv_filename = Column(String)  # hobo
        # csv_modified_timestamp = Column(TIMESTAMP)  # hobo
        # earliest_timestamp = Column(TIMESTAMP)  # hobo

        # def __repr__(self):
        #     return "<DatabaseInsertionTimestamp(id='%s', timestamp='%s, is_success='%s')>" % (
        #                          self.id, self.timestamp, self.is_success)


    class Reading(BASE):
        """
        This class represents the reading table

        The table contains data read by the sensor for given units of time (usually minutes)

        Columns:
            id: uniquely identifies a reading
            sensor_id: the database id of the sensor that made the reading
            timestamp: the reading's timestamp
            unit: corresponds to the column name of the reading
            as obtained in its api request
            upload_timestamp: the time when the api request was made
            value: the numerical value of a reading
        """
        __tablename__ = 'reading'

        id = Column(Integer, primary_key=True)
        sensor_id = Column(Integer)
        timestamp = Column(TIMESTAMP)
        unit = Column(String)
        upload_timestamp = Column(TIMESTAMP)
        value = Column(DOUBLE_PRECISION)


    class Sensor(BASE):
        """
        Sources of readings

        Columns:
            path: used in egauge and webctrl APIs; hobo sensor number
            type: string representing how readings are accessed in ALL CAPS; EGAUGE, WEBCTRL, HOBO
        """
        __tablename__ = 'sensor'

        id = Column(Integer, primary_key=True)
        path = Column(String)
        type = Column(String)
        # sample_resolution = Column(String)
        # is_active  = Column(BOOLEAN)
        # crontab_run_interval = Column(STRING)
        # hawaii_time_difference = Column(Integer) (+10 for egauge, 0 for everything else so far) #alternate name hours_ahead_of_hawaii


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
        BASE.metadata.drop_all(db)
        # Reading.__table__.drop(db)

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
