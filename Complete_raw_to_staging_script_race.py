## Complete script example how to load CSV data in parquet format
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType
from pyspark.sql.functions import col,lit,current_timestamp,concat,to_timestamp
df_race_raw_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                      StructField("year",IntegerType(),True),
                                      StructField("round",IntegerType(),True),
                                      StructField("circuitId",IntegerType(),True),
                                      StructField("first",StringType(),True),
                                      StructField("last",StringType(),True),
                                      StructField("date",DateType(),True),
                                      StructField("time",StringType(),True),
                                      StructField("url",StringType(),True)])
file_path_raw_race=("/FileStore/tables/race_raw_detail.csv")
df_race_raw_read=spark.read.option("header",True).schema(df_race_raw_schema).csv(file_path_raw_race)
#print(df_race_raw_read.dtypes)
df_race_field_add=df_race_raw_read.withColumn("Full_Name",concat(col("first"),lit(" "),col("last"))).withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),'yyyy-MM-dd HH:mm:ss')).withColumn("ingestion_date",current_timestamp())
#display(df_race_field_add)
df_race_field_renamed=df_race_field_add.select(col("raceId").alias("race_id"),col("year").alias("race_year"),col("round").alias("round"),col("circuitId").alias("circuit_id"),col("Full_Name").alias("full_name"),col("race_timestamp"),col("ingestion_date"))
display(df_race_field_renamed)
#print(df_race_field_renamed.dtypes)
df_race_field_renamed.write.mode("overwrite").partitionBy("race_year").parquet("/FileStore/tables/race")
### checking data after load
temp_read=spark.read.parquet("/FileStore/tables/race/race_year=2005")
display(temp_read)

##new code added

%fs
ls /FileStore/tables/race/race_year=2005