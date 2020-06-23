import os
import os.path
import sys
import mysql.connector
import traceback
import logging
from os import listdir, path
from os.path import isfile, join
from time import sleep
from mysql.connector import Error

from properties import FROM_PATH, TO_PATH, ERROR_PATH, LOG_PATH, file_extensions
from db_properties import host, user, passwd


class FilerMover():

    def __init__(self):
        self.check_directory(TO_PATH)
        self.check_directory(ERROR_PATH)
        self.check_directory(LOG_PATH)
        self.logger_setup()

    def move_files(self):
        file_list = [f for f in listdir(FROM_PATH) if isfile(join(FROM_PATH, f))]
        for file in file_list:
            try:
                if file.split(".")[1] in file_extensions:
                    print('Moving {} to {} directory.'.format(file, TO_PATH))
                    os.rename(FROM_PATH + file, TO_PATH + file)
                    sleep(0.5)
                    if path.exists(TO_PATH + file):
                        print("{} moved succesfully.".format(file))
                        sleep(0.5)
                else:
                    print('Files with this extension are not supported. {} has been moved to {} directory.'.format(file, ERROR_PATH))
                    os.rename(FROM_PATH + file, ERROR_PATH + file + 'ExtensionError')
                    sleep(0.5)
            except IndexError:
                print('Files without extension are not supported. {} has been moved to {} directory.'.format(file, ERROR_PATH))
                os.rename(FROM_PATH + file, ERROR_PATH + file + 'ExtensionError')
                sleep(0.5)

    def mysql_insert(self, db):
        try:
            conn = mysql.connector.connect(
                host=host,
                user=user,
                passwd=passwd,
                database=db
            )
            if conn.is_connected():
                try:
                    cur = conn.cursor()
                    cur.execute('SHOW TABLES')
                    result = cur.fetchall()
                    print(result)
                except Error as err:
                    tb = traceback.format_exc()
                    print('An error occured while executing a sql query: {}'.format(err))
                    print(tb)
                finally:
                    if conn.is_connected():
                        cur.close()
                        conn.close()
        except Error as err:
            tb = traceback.format_exc()
            print('An error occured while connecting to database: {}'.format(err))
            print(tb)
        

    def check_directory(self, path):
        if not os.path.exists(path):
            os.mkdir(path)

    
    def logger_setup(self):
        control_file = LOG_PATH + 'file_mover.log'
        logging.basicConfig(
            format='[%(asctime)s][%(process)s][%(levelname)s]: %(message)s',
            level=logging.DEBUG,
            handlers=[
                logging.FileHandler(control_file),
                logging.StreamHandler()
                ]
        )
        logging.info('info')

if __name__ == "__main__":
    mover = FilerMover()
    # while True:
    #     try:
    #         mover.move_files()
    #         sleep(5)
    #     except KeyboardInterrupt as kex:
    #         print(kex)
    #         sys.exit(1)
