import os
import psycopg2
from pyspark.sql import SparkSession

#Function to initialize Spark session
def create_spark_session():
    """Initialize Spark session with PostgreSQL JDBC driver."""
    return SparkSession.builder \
        .appName("MLBAnalytics") \
        .getOrCreate()


#Function to create PostgreSQL tabless
def create_postgres_tables(pg_un, pg_pw):
    """Create PostgreSQL tables if they don't exist."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=pg_un,
            password=pg_pw,
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        create_table_queries = [
            #Master Hitting
            """
            CREATE TABLE IF NOT EXISTS master_hitting (
                id INT,
                Player_name TEXT,
                Position TEXT,
                Games INT,
                AB INT,
                Runs INT,
                Hits INT,
                Double_baseman INT,
                Third_baseman INT,
                Home_run INT,
                RBI INT,
                Walks INT,
                Strikeouts INT,
                Stolen_base INT,
                Caught_stealing INT,
                OBP DOUBLE PRECISION,
                SP DOUBLE PRECISION,
                OPS DOUBLE PRECISION
            );
            """,

            #Master Pitching
            """
            CREATE TABLE IF NOT EXISTS master_pitching (
                id INT,
                Player_name TEXT,
                Win INT,
                Loss INT,
                Earned_run_Average DOUBLE PRECISION,
                Games_played INT,
                Games_started INT,
                Complete_Game INT,
                Shutout INT,
                Save INT,
                Save_Opportunity INT,
                Innings_Pitched DOUBLE PRECISION,
                Hit INT,
                Run INT,
                Earned_run INT,
                Home_run INT,
                Hit_Batsmen INT,
                Base_on_balls INT,
                Strikeouts INT,
                WHIP DOUBLE PRECISION,
                AVG DOUBLE PRECISION
            );
            """,

            #Query Hitting
            """
            CREATE TABLE IF NOT EXISTS query_hitting (
                id INT,
                Player_name TEXT,
                Position TEXT,
                AB INT,
                Hits INT,
                Home_run INT,
                RBI INT,
                OBP DOUBLE PRECISION,
                OPS DOUBLE PRECISION
            );
            """,

            #Query Pitching
            """
            CREATE TABLE IF NOT EXISTS query_pitching (
                id INT,
                Player_name TEXT,
                Games_played INT,
                Win INT,
                Loss INT,
                Earned_run_Average DOUBLE PRECISION,
                Strikeouts INT,
                WHIP DOUBLE PRECISION,
                AVG DOUBLE PRECISION
            );
            """
        ]

        for query in create_table_queries:
            cursor.execute(query)
        conn.commit()

    except Exception as e:
        print(f"[ERROR] Error creating tables: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


#Function to load parquet tables into Spark temp views
def register_temp_views(spark, input_dir):
    """Register Parquet datasets as Spark SQL temp views."""
    tables = [
        ("master_hitting", "master_hitting_table"),
        ("master_pitching", "master_pitching_table"),
        ("hitting", "hitting_table"),
        ("pitching", "pitching_table"),
        ("query_hitting", "query_hitting_table"),
        ("query_pitching", "query_pitching_table")
    ]

    for parquet_path, table_name in tables:
        try:
            path = os.path.join(input_dir, parquet_path)
            df = spark.read.parquet(path)
            df.createOrReplaceTempView(table_name)
            print(f"[INFO] Registered {table_name} from {path}")
        except Exception as e:
            print(f"[ERROR] Could not register {table_name}: {e}")


# Function to load parquet tables into PostgreSQL
def load_to_postgres(spark, input_dir, pg_un, pg_pw):
    """Load MLB Parquet tables to PostgreSQL."""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": pg_un,
        "password": pg_pw,
        "driver": "org.postgresql.Driver"
    }

    tables = [
        ("master_hitting", "master_hitting"),
        ("master_pitching", "master_pitching"),
        ("query_hitting", "query_hitting"),
        ("query_pitching", "query_pitching")
    ]

    for parquet_path, table_name in tables:
        try:
            df = spark.read.parquet(os.path.join(input_dir, parquet_path))
            #Keep master tables as append, query tables overwrite
            mode = "append" if "master" in table_name else "overwrite"
            df.write \
                .mode(mode) \
                .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            print(f"[INFO] Loaded {table_name} into PostgreSQL")
        except Exception as e:
            print(f"[ERROR] Error loading {table_name}: {e}")


if __name__ == "__main__":
    #Get absolute path to ETL/data/parquet
    etl_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(etl_dir, "data", "parquet")

    pg_un = "postgres"
    pg_pw = "123"   

    spark = create_spark_session()

    #Step 1: Create PostgreSQL tables
    create_postgres_tables(pg_un, pg_pw)

    #Step 2: Register Spark temp views 
    register_temp_views(spark, input_dir)

    #Step 3: Load Parquet to PostgreSQL
    load_to_postgres(spark, input_dir, pg_un, pg_pw)
