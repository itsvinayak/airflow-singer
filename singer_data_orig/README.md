### 1.Install tap-mysql(rahul pandit)

```bash
$ python3 -m venv ~/.virtualenvs/tap-mysql
$ source ~/.virtualenvs/tap-mysql/bin/activate
$ pip install tap-mysql

```

### 2.Install target-csv

```bash
$ python3 -m venv ~/.virtualenvs/target-csv
$ source ~/.virtualenvs/target-csv/bin/activate
$ pip install target-csv

```
### 
```

$ python3 -m venv ~/.virtualenvs/target-postgres
$ source ~/.virtualenvs/target-postgres/bin/activate
$ pip install target-postgres
$ pip install singer-target-postgres


```


### 3.Create the configuration file

Create a config file containing the database connection credentials, e.g.:

```json
{
  "host": "localhost",
  "port": "3306",
  "user": "root",
  "password": "password"
}
```

### 4.Discovery mode

The tap can be invoked in discovery mode to find the available tables and
columns in the database:

```bash
$  ~/.virtualenvs/tap-mysql/bin/tap-mysql --config config.json --discover

```

### 5.create state file for increamental

extract data after id 20

```bash
add selected,replication-method and replication-key in db.json
---------------------------------
      "stream": "UserCreditAccount",
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "selected-by-default": false,
            "database-name": "cloutcubetest",
            "row-count": 27,
            "is-view": false,
            "table-key-properties": [
              "CreditId"
            ],
            "selected": true,
            "replication-method": "INCREMENTAL",
            "replication-key": "CreditId"
          }
        },
        {
----------------------------------

state.json file will look like this

{
  "bookmarks": {
    "cloutcubetest-UserCreditAccount": {
      "version":2323,
      "replication_key_value":20,
      "replication_key": "CreditId"
    }
  },
  "currently_syncing": null

}




```

### There are 3 main ways in which we can extract data from the MySQL source, they are
```

    FULL_TABLE: Reads the entire table data from SQL. Eg): SELECT C1, C2 FROM table1;.
    INCREMENTAL: Reads table data from SQL using an ordered key. Eg): SELECT C1, C2 FROM table1 WHERE C1 >= last_read_C1;. Loads incrementally using the last read key.
    LOG_BASED: Reads data directly from the binlog. Loads incrementally using the last read log number. Logs are numbered sequentially in order.
```


### follow this link
```bash
https://www.startdataengineering.com/post/cdc-using-singer/

https://transferwise.github.io/pipelinewise/



#set schema in postgres
please follow this link

https://www.postgresqltutorial.com/postgresql-schema/

0.create schema meta_lab;

1.SET search_path TO meta_lab; 

3.search_path;
then 
\d

SHOW search_path;

#==============================install singer in airflow venv-------------------
pip install pipelinewise-singer-python


```


