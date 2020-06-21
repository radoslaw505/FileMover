import os
import os.path
import sys
from os import listdir, path
from os.path import isfile, join
from time import sleep

from properties import FROM_PATH, TO_PATH, ERROR_PATH, file_extensions


class FilerMover():

    def __init__(self):
        self.check_directory(TO_PATH)
        self.check_directory(ERROR_PATH)

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

    def do_something_with_file(self):
        # For eg change name
        # Read file and throw it to the output
        # Collect file and execute sql
        pass

    def check_directory(self, path):
        if not os.path.exists(path):
            os.mkdir(path)

if __name__ == "__main__":
    mover = FilerMover()
    while True:
        try:
            mover.move_files()
            sleep(5)
        except KeyboardInterrupt as kex:
            print(kex)
            sys.exit(1)
