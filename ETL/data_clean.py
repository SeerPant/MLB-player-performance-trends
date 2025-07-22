from spark_session_setup import create_spark_session
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import row_number 
from pyspark.sql.window import Window

#function that creates a spark session
spark = create_spark_session()

#loading batter data along with their hitting statistics
hitting_df = spark.read.csv("data\\raw\\baseball_hitting.csv", header = True, inferSchema = True)


#handling null values and sentinel values of hitters
hitting_df = hitting_df.dropna()
#print(f"[INFO] The number of null values in hitters df is : {hitting_df.filter(F.col("Player name").isNull()).count()}")
hitting_df = hitting_df.filter( hitting_df["Player name"] != "? Mercado")


#loading pitchers along with their pitching statistics
pitching_df = spark.read.csv("data\\raw\\baseball_pitcher.csv", header = True)
#handling null values of pitchers and clening the data
pitching_df = pitching_df.dropna()
#print(f"[INFO] The number of null values in pitchers df is: {pitching_df.filter(F.col("Player name").isNull()).count()}")
pitching_df = pitching_df.toDF(*[col.strip() for col in pitching_df.columns])
#cleaning sentinel values
cols_to_clean = ["Save Opportunity"]
for c in cols_to_clean:
	pitching_df = pitching_df.withColumn( \
		c, F.when(F.col(c) == "--",None).otherwise(F.col(c))
	)
     

#Adding "id" column to hitting_df_with_id
def add_id_hitter():
    #creating window for row_number()
    window_spec = Window.orderBy("Player name")
    hitting_df_with_id = hitting_df.withColumn("id", row_number().over(window_spec))
    #reordering columns
    cols = hitting_df_with_id.columns 
    reordered_cols = ["id"] + [c for c in cols if c!= "id"]
    final_hitting_df = hitting_df_with_id.select(reordered_cols)

    return final_hitting_df

#Adding "id" column to pitching data
def add_id_pitcher():
    #creating window for row_number()
    window_spec = Window.orderBy("Player name")
    pitching_df_with_id = pitching_df.withColumn("id", row_number().over(window_spec))
    #reordering columns
    cols = pitching_df_with_id.columns 
    reordered_cols = ["id"] + [c for c in cols if c!= "id"]
    final_pitching_df = pitching_df_with_id.select(reordered_cols)

    return final_pitching_df


hitting_df_final = add_id_hitter()
pitching_df = add_id_pitcher()



#Inferring data types of hitting data
hitting_df_final = hitting_df_final.withColumn("Strikeouts",F.col("Strikeouts").cast(T.IntegerType()))
hitting_df_final = hitting_df_final.withColumn(("Caught stealing"), F.col("Caught stealing").cast(T.IntegerType()))

#Inferring data types of pitching data 
pitching_df = pitching_df.withColumn("Win",F.col("Win").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Loss",F.col("Loss").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Earned run Average",F.col("Earned run Average").cast(T.DoubleType()))
pitching_df = pitching_df.withColumn("Games played",F.col("Games Played").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Games started",F.col("Games started").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Complete Game",F.col("Complete Game").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Shutout",F.col("Shutout").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Save",F.col("Save").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Save Opportunity",F.col("Save Opportunity").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Innings Pitched",F.col("Innings Pitched").cast(T.DoubleType()))
pitching_df = pitching_df.withColumn("hit",F.col("hit").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("run",F.col("run").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("earned run",F.col("earned run").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("home run",F.col("home run").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Hit Batsmen",F.col("Hit Batsmen").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("base on balls",F.col("base on balls").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Strikeouts",F.col("Strikeouts").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("WHIP",F.col("WHIP").cast(T.DoubleType()))
pitching_df = pitching_df.withColumn("AVG",F.col("AVG").cast(T.DoubleType()))

#Confirming validity of data and schemas
hitting_df_final.printSchema() 
pitching_df.printSchema()
pitching_df.show()
#hitting_df_final.show() 
#pitching_df.show()
