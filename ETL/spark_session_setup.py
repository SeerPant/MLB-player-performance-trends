from pyspark.sql import SparkSession 

#Function to create a spark session with builder pattern
def create_spark_session():
    sparkSession = SparkSession.builder.appName("MLBAnalytics").getOrCreate()
    return sparkSession

if __name__ == "__main__":
    session = create_spark_session()
    print(f"[INFO] The version of pyspark in use: {session.version}")