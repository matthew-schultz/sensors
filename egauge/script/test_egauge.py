#!./env/bin/python3
"""
Test suite for api_egauge using the unittest module
"""
from freezegun import freeze_time

import arrow
import api_egauge
import orm_egauge
import pandas
import unittest


TEST_DATABASE_URL = 'postgresql:///test'


class TestEgaugeAPI(unittest.TestCase):
    """
    A test suite for api_egauge

    Automatically runs any function starting with 'test_' in its name when main() is called
    Automatically runs setUp() before and tearDown() after each of the 'test' functions run

    Currently has tests for get_data_from_api()
    """
    def setUp(self):
        orm_egauge.setup(TEST_DATABASE_URL)


    def tearDown(self):
        orm_egauge.teardown(TEST_DATABASE_URL)


    # test if two subsequent calls to get_data_from_api() will return duplicate timestamps
    def test_api_requested_data_doesnt_overlap(self):
        conn = api_egauge.get_db_handler(db_url=TEST_DATABASE_URL)
        timestamps = [ arrow.get('2018-02-01T00:00:00-10:00'), arrow.get('2018-02-01T00:05:00-10:00'), arrow.get('2018-02-01T00:10:00-10:00') ]
        egauge_id = 'egauge31871'
        unit_of_time = 'm'
        # Shift timestamp back by 60 seconds when inserting table
        # because get_data_from_api() will use timestamp + 60 seconds
        # for the start time in its api request.
        timestamp_row = orm_egauge.DatabaseInsertionTimestamp(timestamp=timestamps[0].shift(seconds = -60).datetime, is_success=True)
        conn.add(timestamp_row)
        conn.commit()
        # get_data_from_api() adds 60 seconds to api_start_timestamp arg when making egauge api call
        frozen_time1 = timestamps[1].format('YYYY-MM-DD HH:mm:ss ZZ')
        with freeze_time(time_to_freeze=frozen_time1):
            reading_dataframe1 = api_egauge.get_data_from_api(conn, egauge_id, unit_of_time)
        index1 = pandas.Index(reading_dataframe1['Date & Time'])
        #print contents of Date & Time column for visual verification if test fails
        print(index1)

        timestamp_row = orm_egauge.DatabaseInsertionTimestamp(timestamp=timestamps[1].shift(seconds = -60).datetime, is_success=True)
        conn.add(timestamp_row)
        conn.commit()
        frozen_time2 = timestamps[2].format('YYYY-MM-DD HH:mm:ss ZZ')
        with freeze_time(time_to_freeze=frozen_time2):
            reading_dataframe2 = api_egauge.get_data_from_api(conn, egauge_id, unit_of_time)
        index2 = pandas.Index(reading_dataframe2['Date & Time'])
        print(index2)

        frames = [reading_dataframe1, reading_dataframe2]
        concatenated_frame = pandas.concat(frames)
        conn.close()
        self.assertTrue(not any(concatenated_frame['Date & Time'].duplicated()))


    # test get_data_from_api() for missing rows by confirming
    # if the correct number of values are returned from an api request
    def test_api_requested_data_for_missing_rows(self):
        conn = api_egauge.get_db_handler(db_url=TEST_DATABASE_URL)
        start_timestamp = arrow.get('2018-02-01T00:00:00.000-10:00')
        end_timestamp = arrow.get('2018-02-01T00:15:44.100-10:00')
        #insert start_timestamp into database_insertion_timestamp
        timestamp_row = orm_egauge.DatabaseInsertionTimestamp(timestamp=start_timestamp.shift(seconds = -60).datetime, is_success=True)
        conn.add(timestamp_row)
        conn.commit()
        frozen_time = end_timestamp.format('YYYY-MM-DD HH:mm:ss ZZ')
        with freeze_time(time_to_freeze=frozen_time):
            reading_dataframe = api_egauge.get_data_from_api(conn, 'egauge31871', 'm')
        index = pandas.Index(reading_dataframe['Date & Time'])
        print(index)
        # get the difference in minutes between the start and end time
        diff = end_timestamp - start_timestamp
        minutes, remainder = divmod(diff.seconds, 60)
        conn.close()
        # Check if difference in minutes between the start and end
        # matches the number of rows in the reading_dataframe
        self.assertTrue(minutes == reading_dataframe.shape[0])


    """
    unit test example template

    def test_something(self): #name must begin with 'test_' if you want unittest.main() to automatically run it
        # write test code that runs your desired function in a certain way that creates a certain result
        # call an assert function that will return a boolean based on the result of your test code
        self.assertTrue(True)

    """


if __name__ == '__main__':
    unittest.main()
