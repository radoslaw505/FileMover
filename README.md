Before running script you need to:
 - `python -m pip install cx_oracle --upgrade` 
 - or `python -m pip install mysql-connector-python`

Create a properties.py file with paths and file details:
``` 
 FROM_PATH = 'C:/Users/path/to/move_from/directory/' # Only this directory need to be created before running script
 TO_PATH = 'C:/Users/path/to/move_to/directory/'
 ERROR_PATH = 'C:/Users/path/to/error/directory/'
 LOG_PATH = 'C:/Users/path/to/log/directory/'
 file_extensions = ['txt', 'csv'] # List 
 file_delimiter = ',' # String
```

Depending on what databases you use create oracle_config.py for Oracle Database:
```
user = 'your_username'
passwd = 'your_password'
host = 'your_host' # eg. 'localhost'
port = 'yout_port_number' # eg. 1521
sid = 'your_database_sid' # eg. 'orcl'
oracle_query = 'INSERT INTO employees2 (first_name, last_name) VALUES (:1, :2)' # It can be update/insert
val_num = 2 # Number of values to insert/update - for checking input format
```

or db_properties.py for MySQL Database:
```
user = 'your_username'
passwd = 'your_password'
host = 'your_host'
database = 'your_database'
mysql_query = "INSERT INTO employees2 (first_name, last_name) VALUES (%s, %s)"
val_num = 2
```


Running script:
```
python .\file_mover.py
```

Script will create a class `mover` and execute a `move_file()` method that will repeat every 5 seconds.
Steps that the script performs:
- checks if directories from properties.py exists, if not he will create them,
- checks if extensions are supported,
- reads files from `FROM_PATH` directory and converts them into a tuple or list of tuples,
- checks if the file format is correct or file is not empty,
- runs queries on the database,
- processed files are moved to `TO_PATH`, errored to `ERROR_PATH`.
