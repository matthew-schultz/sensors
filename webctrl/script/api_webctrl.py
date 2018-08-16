#!../../egauge/script/env/bin/python3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import func #used for sql max() function

import arrow
import json
import orm_webctrl
import requests
import sys
import traceback

ERROR_LOG = 'error.log'

def pull_webctrl_data(sensor_id = 'ABSPATH:1:#powerscout_frog_2g/kw_avg_phase_b_tn', db_url='postgresql:///webctrl', unit_of_time='m', output_file='./output.txt'):
    try:  # should catch any initial database connection errors etc
        is_success = False
        current_time = arrow.now()

        # connect to the database
        db = create_engine(db_url)
        Session = sessionmaker(db)
        session = Session()

        latest_retrieved_time = ''
        last_inserted_time = ''

        #get webctrl user information
        webctrl_user_row = session.query(orm_webctrl.WebctrlUser.username, orm_webctrl.WebctrlUser.password).first()
        # returns a TypeError if there are no users in database
        api_user = webctrl_user_row[0]
        api_pass = webctrl_user_row[1]

        try: # this try-except block should catch any webctrl sensor api connection errors
            # get the latest timestamp
            max_timestamp_row = session.query(func.max(orm_webctrl.DatabaseInsertionTimestamp.timestamp)).filter_by(is_success=True)
            # if no timestamp is found, insert the current time and skip egauge api request, etc.
            if not max_timestamp_row[0][0]:
                is_success = True
            else:
                host = 'http://www.soest.hawaii.edu/hneienergy/bulktrendserver/read'
                latest_retrieved_time = arrow.get(max_timestamp_row[0][0])
                start_date = latest_retrieved_time.format('YYYY-MM-DD')
                end_date = current_time.format('YYYY-MM-DD')
                output_format = 'json'
                params = {'id': sensor_id, 'start': start_date, 'end': end_date, 'format': output_format}
                auth = (api_user, api_pass)
                request = requests.post(host, params=params, auth=tuple(auth))

                if request.status_code == requests.codes.ok:
                    print('Request was successful' + str(request))
                    rows_inserted = 0
                    sensor_json_data = request.json()
                    sensor_path = sensor_json_data[0]['id']
                    readings = sensor_json_data[0]['s']
                    #TEST
                    print('len(readings): ',str(len(readings)))
                    for reading in readings:
                        reading_timestamp = ''
                        reading_units = ''
                        reading_datatype = ''
                        reading_value = ''

                        for key in reading.keys():
                            if key is 't':
                                reading_timestamp = reading[key]
                            elif key is 'a':
                                reading_datatype = 'a'
                                reading_value = reading[key]
                            elif key is 'd':
                                reading_datatype = 'd'
                                reading_value = reading[key]
                        # slice off extra digits since arrow uses 10 digit timestamps
                        reading_time = arrow.get(str(reading_timestamp)[:10])

                        # shift reading time back by 10 hours because local time is GMT-10
                        if(reading_time.shift(hours = -10) > latest_retrieved_time):
                            reading_row = orm_webctrl.Reading(sensor_path=sensor_path, timestamp=reading_time.datetime, units=reading_datatype, reading=str(reading_value), upload_timestamp=current_time.datetime)
                            session.add(reading_row)
                            rows_inserted += 1
                            last_inserted_time = reading_time
                    with open(output_file, 'w') as outfile:
                        json.dump(sensor_json_data, outfile, indent=4)
                    #mark that all data has been successfully inserted
                    if rows_inserted > 0:
                        is_success = True
                        print(rows_inserted, ' rows inserted')
                else: #handle errors connecting to webctrl api
                    print('Connection failure' + str(request.status_code))
        #catch webctrl api request exceptions like requests.exceptions.ConnectionError
        except Exception as e:
            error_msg = str(traceback.format_exc())
            print(error_msg)
            with open(ERROR_LOG, 'a+') as error_file:
                error_time_readable = arrow.now().format('ddd MMM DD YYYY HH:mm:ss ZZ')
                error_file.write('[' + error_time_readable + ']')
                error_file.write(error_msg + '\n')
        #insert current time into database_insertion_timestamp table
        if last_inserted_time:
            datetime_to_insert = last_inserted_time.datetime
        else:
            datetime_to_insert = current_time.datetime
        database_insertion_timestamp_row = orm_webctrl.DatabaseInsertionTimestamp(timestamp=datetime_to_insert, is_success=is_success)
        session.add(database_insertion_timestamp_row)

        session.commit()
        session.close()
    #catch ORM exeptions like sqlalchemy.exc.InternalError, sqlalchemy.exc.OperationalError, TypeError (if no users in database)
    except Exception as e:
        error_msg = str(traceback.format_exc())
        print(error_msg)
        with open(ERROR_LOG, 'a+') as error_file:
            error_time_readable = arrow.now().format('ddd MMM DD YYYY HH:mm:ss ZZ')
            error_file.write('[' + error_time_readable + ']')
            error_file.write(error_msg + '\n')

if __name__=='__main__':
    pull_webctrl_data(*sys.argv[1:])
