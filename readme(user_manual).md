Overview
This project contains process of Extracting, Transforing and Loading (ETL) Major League Baseball's (MLB) Hitting and Pitching data.

This has 3 main scripts:

1. Extract.py: Downloads and reads the raw dataset
2. Transform.py : Cleans and standardizes the dataset
3. Load.py: Loads the processed data into postgreSQL tables.

Final result will be a structured databse with:

1. Master tables: stores the full cleaned dataset.
2. Query tables: generally contains selected subsets or aggregated data.

Prequisites (These need to be present in the system compulsorily):

1. Python 3.11+
2. PostgresSQL
3. PySpark

Dependencies:

1. Python os library
2. KaggleApi authenticator
3. psycopg2

Database

1. PostgreSQL server must be running
2. Connect to a database
3. Connection details, the host, user, password and database name

To download and use this ETL pipeline:

1. Go to terminal and type:
   git clone "https://github.com/SeerPant/MLB-player-performance-trends.git"

2. Run extract.py , transform.py and load.py in order.
   SEE 2.1 FOR DETAILS REGARDING THE LOADING PROCESS.
   Extract extracts the dataset from kaggle into data/raw.  
   Transform cleans the dataset, standardizes column names and saves as a parquet file in data/cleaned and data/parquet.
   Load loads the cleaned data into postgreSQL in form of master and query optimized tables.

2.1 To load into postgres, the provided credentials should match before running load.py.
DATABASE = "mlb_db" (or any name that you give)
USER = "postgres" (or any name that you give)
PASSWORD = "your password" (the password that you created)
HOST = "localhost" (or any name that you give)
PORT = "5432" (Which is the default port for postgreSQL)

3. The postgresSQL database will contatin: Master table and query tables. Which cna be checked in pgadmin by navigating to the 'localserver' -> 'Schemas' then 'tables'.

4. Conduct visualization how ever you want.
   A simple example would be to use power BI,
   load the cleaned datasets "Hitting and Pitching" from data/cleaned and create charts.

License:
© 2025 Seer Pant. All rights reserved.
This repository contains original code written by me and makes use of:
Open-source python libraries
Data sourced from kaggle - OWNED AND LICENSED BY THE ORIGINAL UPLOADER. Check the dataset's Kaggle page for its specific license and terms of use.
The dataset is not included in this license. You must comply with the dataset provider's license if you use it.
