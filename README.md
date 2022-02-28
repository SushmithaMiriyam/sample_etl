# Sample ETL script

This repository contains scripts that sets up SQLite Database and loads data
into it.

## Notes
The `requirements.txt` lists all Python libraries that the script requires

```
pip3 install -r requirements.txt
```

initialize_database.py
- modules to create SQLite database and table

etl.py
- setup SQLite database if it does not exist.
- Extracts, Transforms and Loads datafile into sqlite3 database

```
python etl.py --data_file=<path_to_csv_file>
```

Assumption:
the input datafile has the following header:
id,first_name,last_name,email,gender,ip_address
