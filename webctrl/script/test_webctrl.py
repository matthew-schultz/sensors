#!/usr/bin/python3
"""
Test suite for api_webctrl using the unittest module

WARNING: running this test modifies the postgresql database named 'test'
REQUIRES A test_webctrl.txt file with webctrl username and password
"""
from freezegun import freeze_time

import api_webctrl
import configparser
import orm_webctrl
import pendulum
import unittest


class TestWebctrlAPI(unittest.TestCase):
    """
    Automatically runs any function starting with 'test_' in its name when main() is called
    Automatically runs setUp() before and tearDown() after each of the 'test' functions run

    Currently has tests for handling webctrl rows with duplicate datetimes
    """
    def setUp(self):
        # set up connection to database 'test'
        self.db_url = 'postgresql:///test'
        self.db = api_webctrl.create_engine(self.db_url)
        Session = api_webctrl.sessionmaker(self.db)
        self.conn = Session()

        # create all tables defined in orm_webctrl in database 'test'
        orm_webctrl.BASE.metadata.create_all(self.db)
        # read and insert webctrl api credentials from text file
        config_path = "test_webctrl_config.txt"
        with open(config_path, "r") as file:
            # prepend '[DEFAULT]\n' since ConfigParser requires section headers in config files
            config_string = '[DEFAULT]\n' + file.read()
        config = configparser.ConfigParser()
        config.read_string(config_string)
        username = config['DEFAULT']['username']
        password = config['DEFAULT']['password']
        self.conn.add(orm_webctrl.ApiAuthentication(username=username, password=password))


    def tearDown(self):
        # drop all tables in database 'test'
        orm_webctrl.BASE.metadata.drop_all(self.db)
        # close connection to database 'test'
        self.conn.close()


    # test if api_webctrl removes duplicate readings (same timestamp) before successfully inserting readings into db
    # occasionally the webctrl server may have duplicate readings with the same timestamp
    def test_handling_duplicate_rows(self):
        # use Fri Jun 14 2019 12:35:00 as start time since duplicate readings for this sensor starting <3 minutes after
        sensor = orm_webctrl.SensorInfo(query_string='ABSPATH:1:#frog2_room_sensors/zone_humidity_2_tn',
                                        last_updated_datetime=pendulum.parse('2019-06-14T12:35:00.000-10:00'),
                                        purpose_id=8, is_active=True, script_folder='webctrl', unit='%')
        self.conn.add(sensor)

        # use freeze time to set current time to later on that same day
        frozen_time = pendulum.parse('2019-06-14T01:00:00.000-10:00')
        with freeze_time(time_to_freeze=frozen_time):
            # grab webctrl readings for sensor from 12:35pm to 1pm on 6/14/2019
            readings = api_webctrl.get_data_from_api(sensor, self.conn)
            # attempt to insert
            api_webctrl.insert_readings_into_database(self.conn, readings, sensor)

        test_was_successful = False
        # use query to check if any readings were successfully inserted
        # stored query() result in inserted_reading variable since query() broke test when inside if statement
        inserted_reading = self.conn.query(orm_webctrl.Reading.datetime).first()
        self.conn.commit()
        # if readings were successfully inserted, test succeeded!
        if inserted_reading:
            test_was_successful = True
        self.assertTrue(test_was_successful)


    """
    unit test example template

    def test_something(self): #name must begin with 'test_' if you want unittest.main() to automatically run it
        # write test code that runs your desired function in a certain way that creates a certain result
        # call an assert function that will return a boolean based on the result of your test code
        self.assertTrue(True)

    """


# check that test_webctrl_config.txt has webctrl username and password before running tests
def check_config():
    # read api_authentication from text file
    config_path = 'test_webctrl_config.txt'
    with open(config_path, 'r') as file:
        # prepend '[DEFAULT]\n' since ConfigParser requires section headers in config files
        config_string = '[DEFAULT]\n' + file.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    username = config['DEFAULT']['username']
    password = config['DEFAULT']['password']

    if username and password:
        return True
    else:
        return False


if __name__ == '__main__':
    # run tests if config is set
    if check_config():
        unittest.main()
    else:
        print('Add webctrl api username and password to webctrl/script/test_webctrl_config.txt')
