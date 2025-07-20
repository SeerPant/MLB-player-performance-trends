from spark_session_setup import create_spark_session
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import row_number 
from pyspark.sql.window import Window


spark = create_spark_session()

#loading batter data along with their hitting statistics
hitting_df = spark.read.csv("data\\raw\\baseball_hitting.csv", header = True, inferSchema = True)


#handling null values and sentinel values
hitting_df = hitting_df.dropna()
print(f"[INFO] The number of null values is: {hitting_df.filter(F.col("Player name").isNull()).count()}")
hitting_df = hitting_df.filter( hitting_df["Player name"] != "? Mercado")



#loading pitchers along with their pitching statistics
pitching_df = spark.read.csv("data\\raw\\baseball_pitcher.csv", header = True)

#viewing schemas 
#hitting_df.printSchema()

#Adding "id" column to hitting_df
def add_id_column():
    #creating window for row_number()
    window_spec = Window.orderBy("Player name")
    hitting_df_with_id = hitting_df.withColumn("id", row_number().over(window_spec))
    #reordering columns
    cols = hitting_df_with_id.columns 
    reordered_cols = ["id"] + [c for c in cols if c!= "id"]
    final_hitting_df = hitting_df_with_id.select(reordered_cols)

    return final_hitting_df


hitting_df_final = add_id_column()

#Dropping irrelevant rows 
hitting_df_final.printSchema()
hitting_df_final.show() 


