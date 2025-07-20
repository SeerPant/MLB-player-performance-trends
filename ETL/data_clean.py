from spark_session_setup import create_spark_session

spark = create_spark_session()

#loading batter data along with their hitting statistics
hitting_df = spark.read.csv("data\\raw\\baseball_hitting.csv", header = True)

#loading pitchers along with their pitching statistics
pitching_df = spark.read.csv("data\\raw\\baseball_pitcher.csv", header = True)

#viewing schemas 
#hitting_df.printSchema()
hitting_df.show()
