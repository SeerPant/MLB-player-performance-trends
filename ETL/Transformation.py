import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import row_number 
from pyspark.sql.window import Window


#Function to create a spark session with builder pattern
def create_spark_session():
    spark = SparkSession.builder.appName("MLBAnalytics").getOrCreate()
    return spark



#function that creates a spark session
spark = create_spark_session()

#creating file paths 
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")
PARQUET_DIR = os.path.join(BASE_DIR, "data", "parquet")

os.makedirs(CLEANED_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

"""CLEANING HITTING DATA"""
#loading raw data
hitting_df = spark.read.csv(os.path.join(RAW_DIR, "baseball_hitting.csv"), header=True, inferSchema=True)
pitching_df = spark.read.csv(os.path.join(RAW_DIR, "baseball_pitcher.csv"), header=True, inferSchema=True)

#handling null values and sentinel values of hitters
hitting_df = hitting_df.dropna()

#cleaning sentinel values
cols_to_clean = ["Caught stealing"]
for ch in cols_to_clean:
	hitting_df = hitting_df.withColumn( \
		ch, F.when(F.col(ch) == "--",None).otherwise(F.col(ch))
	)
#print(f"[INFO] The number of null values in hitters df is : {hitting_df.filter(F.col("Player name").isNull()).count()}")
hitting_df = hitting_df.filter( hitting_df["Player name"] != "? Mercado")
#handling spaces column names 
hitting_df = hitting_df.toDF(*[col.strip() for col in hitting_df.columns])



"""CLEANING PITCHER DATA"""

#handling null values of pitchers and cleaning the data
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
pitching_df= add_id_pitcher()

#dropping previously added snake case columns
#for hitting data
hitting_snake_case_cols = ['Player_name',"AB","Double","Third_baseman","Home_run","RBI","Walks","Stolen_base","SP","OPS"]

for c in hitting_snake_case_cols:
      if c in hitting_df_final.columns:
           hitting_df_final = hitting_df_final.drop(c)

#for pitching data
pitching_snake_case_cols = ['Earned_run_Average',"Games_played","Games_started","Complete_Game","Save_Opportunity","Innings_Pitched","Earned_run","Home_run","Hit_Batsmen","Base_on_balls"]

for c in pitching_snake_case_cols: 
      if c in pitching_df.columns:
            pitching_df = pitching_df.drop(c)

#Inferring data types of hitting data
hitting_df_final = hitting_df_final.withColumn("Player_name",F.col("`Player name`").cast(T.StringType()))
hitting_df_final = hitting_df_final.drop("Player name")
hitting_df_final = hitting_df_final.withColumn("Position",F.col("`Position`").cast(T.StringType()))
hitting_df_final = hitting_df_final.withColumn("Games",F.col("`Games`").cast(T.IntegerType()))
hitting_df_final = hitting_df_final.withColumn("AB",F.col("`At-bat`").cast(T.IntegerType()))
hitting_df_final = hitting_df_final.drop("At-bat")
hitting_df_final = hitting_df_final.withColumn("Runs",F.col("`Runs`").cast(T.IntegerType()))
hitting_df_final = hitting_df_final.withColumn("Hits",F.col("`Hits`").cast(T.IntegerType()))
hitting_df_final = hitting_df_final.withColumn("Double_baseman",F.col("`Double (2B)`").cast(T.IntegerType()))
hitting_df_final = hitting_df_final.drop("Double (2B)")
hitting_df_final = hitting_df_final.withColumn("Third_baseman",F.col("`third baseman`").cast(T.IntegerType()))
hitting_df_final = hitting_df_final.drop("third baseman")
hitting_df_final = hitting_df_final.withColumn("Home_run",F.col("`home run`").cast(T.IntegerType()))
hitting_df_final = hitting_df_final.drop("home run")
hitting_df_final = hitting_df_final.withColumn("RBI",F.col("`run batted in`").cast(T.IntegerType()))
hitting_df_final = hitting_df_final.drop("run batted in")
hitting_df_final = hitting_df_final.withColumn("Walks",F.col("`a walk`").cast(T.IntegerType()))
hitting_df_final = hitting_df_final.drop("a walk")
hitting_df_final = hitting_df_final.withColumn("Strikeouts",F.col("Strikeouts").cast(T.IntegerType()))
hitting_df_final = hitting_df_final.withColumn("Stolen_base",F.col("`stolen base`").cast(T.IntegerType()))
hitting_df_final = hitting_df_final.drop("stolen base")
hitting_df_final = hitting_df_final.withColumn("Caught_stealing", F.col("`Caught stealing`").cast(T.IntegerType()))
hitting_df_final = hitting_df_final.drop("Caught stealing")
hitting_df_final = hitting_df_final.withColumn("OBP", F.col("`On-base Percentage`").cast(T.DoubleType()))
hitting_df_final = hitting_df_final.drop("On-base Percentage")
hitting_df_final = hitting_df_final.withColumn("SP", F.col("`Slugging Percentage`").cast(T.DoubleType()))
hitting_df_final = hitting_df_final.drop("Slugging Percentage")
hitting_df_final = hitting_df_final.withColumn("OPS",F.col("`On-base Plus Slugging`").cast(T.DoubleType()))
hitting_df_final = hitting_df_final.drop("On-base Plus Slugging")

#Inferring data types of pitching data 
pitching_df = pitching_df.withColumn("Player_name", F.col("`Player name`").cast(T.StringType()))
pitching_df = pitching_df.drop("Player name")
pitching_df = pitching_df.withColumn("Win",F.col("Win").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Loss",F.col("Loss").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Earned_run_Average",F.col("`Earned run Average`").cast(T.DoubleType()))
pitching_df= pitching_df.drop("Earned run Average")
pitching_df = pitching_df.withColumn("Games_played",F.col("`Games played`").cast(T.IntegerType()))
pitching_df = pitching_df.drop("Games played")
pitching_df = pitching_df.withColumn("Games_started",F.col("`Games started`").cast(T.IntegerType()))
pitching_df = pitching_df.drop("Games started")
pitching_df = pitching_df.withColumn("Complete_Game",F.col("`Complete Game`").cast(T.IntegerType()))
pitching_df = pitching_df.drop("Complete Game")
pitching_df = pitching_df.withColumn("Shutout",F.col("Shutout").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Save",F.col("Save").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Save_Opportunity",F.col("`Save Opportunity`").cast(T.IntegerType()))
pitching_df = pitching_df.drop("Save Opportunity")
pitching_df = pitching_df.withColumn("Innings_Pitched",F.col("`Innings Pitched`").cast(T.DoubleType()))
pitching_df = pitching_df.drop("Innings Pitched")
pitching_df = pitching_df.withColumn("Hit",F.col("hit").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Run",F.col("run").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("Earned_run",F.col("`earned run`").cast(T.IntegerType()))
pitching_df = pitching_df.drop("earned run")
pitching_df = pitching_df.withColumn("Home_run",F.col("`home run`").cast(T.IntegerType()))
pitching_df = pitching_df.drop("home run")
pitching_df = pitching_df.withColumn("Hit_Batsmen",F.col("`Hit Batsmen`").cast(T.IntegerType()))
pitching_df = pitching_df.drop("Hit Batsmen")
pitching_df = pitching_df.withColumn("Base_on_balls",F.col("`base on balls`").cast(T.IntegerType()))
pitching_df = pitching_df.drop("base on balls")
pitching_df = pitching_df.withColumn("Strikeouts",F.col("Strikeouts").cast(T.IntegerType()))
pitching_df = pitching_df.withColumn("WHIP",F.col("WHIP").cast(T.DoubleType()))
pitching_df = pitching_df.withColumn("AVG",F.col("AVG").cast(T.DoubleType()))


"""#Saving the cleaned data as csv to proper locations
os.makedirs("data\\cleaned", exist_ok = True)
#writing clean datasets to csv 
hitting_df_final.write.mode("overwrite").option("header", True).csv("data\\cleaned\\hitting_cleaned")

pitching_df.write.mode("overwrite").option("header", True).csv("data\\cleaned\\pitching_cleaned")""" 

#saving cleaned CSVs 
hitting_df_final.write.mode("overwrite").option("header", True).csv(os.path.join(CLEANED_DIR, "hitting_cleaned"))
pitching_df.write.mode("overwrite").option("header", True).csv(os.path.join(CLEANED_DIR, "pitching_cleaned"))


#Write stage 1, writing to parquet 
hitting_df_final.write.mode("overwrite").parquet(os.path.join(PARQUET_DIR, "hitting"))
pitching_df.write.mode("overwrite").parquet(os.path.join(PARQUET_DIR, "pitching"))

#Write stage2, creating a master table 

#Hitting Master Table
hitting_master = hitting_df_final.select(
    "id", "Player_name", "Position", "Games", "AB", "Runs", "Hits", "Double_baseman",
    "Third_baseman", "Home_run", "RBI", "Walks", "Strikeouts", "Stolen_base",
    "Caught_stealing", "OBP", "SP", "OPS"
)
hitting_master.write.mode("overwrite").parquet(os.path.join(PARQUET_DIR, "master_hitting"))

#Pitching Master Table
pitching_master = pitching_df.select(
    "id", "Player_name", "Win", "Loss", "Earned_run_Average", "Games_played",
    "Games_started", "Complete_Game", "Shutout", "Save", "Save_Opportunity",
    "Innings_Pitched", "Hit", "Run", "Earned_run", "Home_run", "Hit_Batsmen",
    "Base_on_balls", "Strikeouts", "WHIP", "AVG"
)
pitching_master.write.mode("overwrite").parquet(os.path.join(PARQUET_DIR, "master_pitching"))

# --- Query Tables ---
# Hitting Query Table
hitting_query = hitting_df_final.select("id", "Player_name", "Position", "AB", "Hits", "Home_run", "RBI", "OBP", "OPS")
hitting_query.write.mode("overwrite").parquet(os.path.join(PARQUET_DIR, "query_hitting"))

# Pitching Query Table
pitching_query = pitching_df.select("id", "Player_name", "Games_played", "Win", "Loss", "Earned_run_Average", "Strikeouts", "WHIP", "AVG")
pitching_query.write.mode("overwrite").parquet(os.path.join(PARQUET_DIR, "query_pitching"))

print("[INFO] Hitting and Pitching master/query tables saved separately as Parquet.")



