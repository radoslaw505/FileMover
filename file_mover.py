import os
import os.path
import sys
import mysql.connector
import cx_Oracle
from os import listdir, path
from os.path import isfile, join
from time import sleep
from mysql.connector import Error

from properties import FROM_PATH, TO_PATH, ERROR_PATH, LOG_PATH, file_extensions, file_delimiter
from db_properties import host, user, passwd, database, mysql_query, val_num
from oracle_config import user, passwd, host, port, sid, oracle_query, val_num
from logging_properties import LoggerSetup

# Create logger / You need to pass a control file name (without extension)
log = LoggerSetup().get_logger('file_mover')

class FilerMover():

    def __init__(self):
        self.check_directory(TO_PATH)
        self.check_directory(ERROR_PATH)


    def move_files(self):
        file_list = [f for f in listdir(FROM_PATH) if isfile(join(FROM_PATH, f))]
        
        for file in file_list:
            try:
                if file.split(".")[1] in file_extensions:
                    filename = FROM_PATH + file
                    result = self.read_file(filename, file_delimiter)
                    if result is None:
                        log.warning('{} has nothing to process. File has been moved to {} directory.'.format(file, ERROR_PATH))
                        os.rename(FROM_PATH + file, ERROR_PATH + file + 'EmptyFileError')
                        continue
                    elif isinstance(result, tuple) and sum(1 for rec in result) != val_num:
                        log.warning("{} has bad format, and can't be process. File has been moved to {} directory.".format(file, ERROR_PATH))
                        os.rename(FROM_PATH + file, ERROR_PATH + file + 'BadFormatError')
                        continue
                    elif isinstance(result, list):
                        for rec in result:
                            rec_num = sum(1 for n in rec)
                            if rec_num != val_num:
                                log.warning("{} has bad format, and can't be process. File has been moved to {} directory.".format(file, ERROR_PATH))
                                os.rename(FROM_PATH + file, ERROR_PATH + file + 'BadFormatError')
                                continue
                    # self.mysql_insert(result)
                    self.oracle_insert(result)
                    log.info('Moving {} to {} directory.'.format(file, TO_PATH))
                    os.rename(FROM_PATH + file, TO_PATH + file)
                    sleep(0.5)
                    if path.exists(TO_PATH + file):
                        log.info("{} moved succesfully.".format(file))
                        sleep(0.5)
                else:
                    log.warning('{} Files with this extension are not supported. File has been moved to {} directory.'.format(file, ERROR_PATH))
                    os.rename(FROM_PATH + file, ERROR_PATH + file + 'ExtensionError')
                    sleep(0.5)
            except IndexError:
                log.warning('{} Files without extension are not supported. File has been moved to {} directory.'.format(file, ERROR_PATH))
                os.rename(FROM_PATH + file, ERROR_PATH + file + 'ExtensionError')
                sleep(0.5)


    # Need to pass: 1 argument - input for insert
    def mysql_insert(self, result):
        log.debug('Calling mysql_insert() method.')
        try:
            conn = mysql.connector.connect(
                host=host,
                user=user,
                passwd=passwd,
                database=database
            )
            if conn.is_connected():
                try:
                    cur = conn.cursor()
                    sql = mysql_query
                    if isinstance(result, list):
                        cur.executemany(sql, result)
                    elif isinstance(result, tuple):
                        cur.execute(sql, result)
                    else:
                        log.warning('Bad format for request.')
                    conn.commit()
                    log.info('{} record inserted.'.format(cur.rowcount))
                except Error as err:
                    log.error('An error occured while executing a sql query: {}'.format(err), exc_info=True)
                finally:
                    if conn.is_connected():
                        cur.close()
                        conn.close()
        except Error as err:
            log.error('An error occured while connecting to database: {}'.format(err), exc_info=True)
            result=[]
            return result
        

    def oracle_insert(self, result):
        log.debug('Calling oracle_insert() method.')
        try:
            dsn = cx_Oracle.makedsn(host, port, sid)
            conn = cx_Oracle.connect(
                user = user,
                password = passwd, 
                dsn = dsn
            )
            cur = conn.cursor()
            sql = oracle_query
            if isinstance(result, list):
                cur.executemany(sql, result)
            elif isinstance(result, tuple):
                cur.execute(sql, result)
            conn.commit()
            log.info('{} record inserted.'.format(cur.rowcount))
        except Exception as ex:
            log.error('An error occured while connecting to database: {}'.format(ex), exc_info=True)
            result=[]
            return result
        finally:
            try:
                cur.close()
                conn.close()
            except NameError as nerr:
                pass


    # Need to pass: 1 argument - directory path
    def check_directory(self, path):
        log.debug('Calling check_directory() method for {}.'.format(path))
        if not os.path.exists(path):
            os.mkdir(path)
            log.info('Directory {} created.'.format(path))

    
    # Need to pass: 1 argument - file path
    def read_file(self, file, delimiter):
        log.debug('Calling read_file() method on "{}" with delimiter "{}".'.format(file.split('/')[-1], delimiter))
        result=[]
        try:
            num_lines = sum(1 for line in open(file))
            if num_lines > 1:
                with open(file) as f:
                    for line in f:
                        line = line.split(delimiter)
                        line[1] = line[1].rstrip()
                        if line is None:
                            continue
                        else:
                            line = tuple(line)
                            result.append(line)
                return result
            elif num_lines == 0:
                pass
            else:
                with open(file) as f:
                    for line in f:
                        line = line.split(delimiter)
                        line[1] = line[1].rstrip()
                        result = tuple(line)
                return result
        except FileNotFoundError as fnerr:
            log.error('An error occured while reading a file: {}'.format(fnerr), exc_info=True)


if __name__ == "__main__":
    mover = FilerMover()
    while True:
        try:
            mover.move_files()
            sleep(5)
        except Exception as ex:
            log.error('An error occured while executing a move_files() method.', exc_info=True)
            sys.exit(1)
