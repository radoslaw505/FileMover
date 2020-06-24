import os
import os.path
import sys
import mysql.connector
import logging
import time
from os import listdir, path
from os.path import isfile, join
from time import sleep
from mysql.connector import Error

from properties import FROM_PATH, TO_PATH, ERROR_PATH, LOG_PATH, file_extensions
from db_properties import host, user, passwd
from logging_properties import LoggerSetup

# Create and import logger
log = LoggerSetup().get_logger('file_mover.log')

class FilerMover():

    def __init__(self):
        self.check_directory(TO_PATH)
        self.check_directory(ERROR_PATH)
        self.check_directory(LOG_PATH)


    def move_files(self):
        file_list = [f for f in listdir(FROM_PATH) if isfile(join(FROM_PATH, f))]
        for file in file_list:
            try:
                if file.split(".")[1] in file_extensions:
                    log.info('Moving {} to {} directory.'.format(file, TO_PATH))
                    os.rename(FROM_PATH + file, TO_PATH + file)
                    sleep(0.5)
                    if path.exists(TO_PATH + file):
                        log.info("{} moved succesfully.".format(file))
                        sleep(0.5)
                else:
                    log.warning('Files with this extension are not supported. {} has been moved to {} directory.'.format(file, ERROR_PATH))
                    os.rename(FROM_PATH + file, ERROR_PATH + file + 'ExtensionError')
                    sleep(0.5)
            except IndexError:
                log.warning('Files without extension are not supported. {} has been moved to {} directory.'.format(file, ERROR_PATH))
                os.rename(FROM_PATH + file, ERROR_PATH + file + 'ExtensionError')
                sleep(0.5)


    # Need to pass: 1 argument - database name 
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
                    cur.execute('SHOW TABLS')
                    result = cur.fetchall()
                    print(result)
                except Error as err:
                    log.error('An error occured while executing a sql query: {}.'.format(err), exc_info=True)


                finally:
                    if conn.is_connected():
                        cur.close()
                        conn.close()
        except Error as err:
            log.error('An error occured while connecting to database: {}'.format(err), exc_info=True)
        

    # Need to pass: 1 argument - directory path
    def check_directory(self, path):
        if not os.path.exists(path):
            os.mkdir(path)


if __name__ == "__main__":
    mover = FilerMover()
    while True:
        try:
            mover.move_files()
            sleep(5)
        except Exception as ex:
            log.error(exc_info=True)
