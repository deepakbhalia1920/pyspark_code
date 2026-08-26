### creating dataFrames from CSV file.

csv_filepath="/FileStore/tables/Department_detail.csv"
df_csv=spark.read.csv(csv_filepath,header=True)
df_csv.show()

## Creating dataFrames with existing tables.

from pyspark.sql.functions import col 

df_table=spark.table("DB_test.department_dtl")
df_table.show()
df_query=spark.sql("select * from DB_test.department_dtl where deptno<=15")
df_query.show()
df_query.count()
### to see limited rows of table
df_query.limit(3).show()
## to see columns of table.
df_query.columns
## to see the datatype of columns
df_query.dtypes
## to get more detail about schema
df_query.printSchema()

#display(df_query)

##to describe any column
from pyspark.sql.functions import col
df_query.describe("deptno").show()

#### csv file import the data
from pyspark.sql.functions import col

csv_filepath='/FileStore/tables/Department_detail-1.csv'
df_csv=spark.read.csv(csv_filepath,header=True)
df_csv.show()

df_select=df_csv.select("deptno","deptname")
df_select.show()
col_lst=["deptno","deptname","deptlocation"]
df_select_col=df_csv.select(col_lst).show()
df_select_col_fun=df_csv.select(col("deptno"),col("deptname")).show()
df_select_col_alias=df_csv.select(col("deptno").alias("department_No"),col("deptname").alias("Department_Name")).show()
#df_select_col_fun.show()
#df_unique=df_select_col_fun.distinct()
df_where=df_select_col.filter("deptno >= 40")
df_where.show()

from pyspark.sql.functions import col

csv_filepath='/FileStore/tables/Department_detail-1.csv'
df_csv=spark.read.csv(csv_filepath,header=True)
df_csv.show()
df_select=df_csv.select("deptno","deptname")
df_select.show()
## we can also use filter to filter the data
df_csv_filter=df_select.filter("deptno=40")
df_csv_filter.sort("deptno").show()
## From this way we can also use where claues to filter the data
df_csv_where=df_select.where("deptno>=30")
df_csv_where.sort("deptno").show()
######to remove duplicates.
df_csv_duplicate=df_select.dropDuplicates()
df_csv_duplicate.sort("deptno").show()

####to have unique values
#df_csv_distinct=df_select.distinct()
#df_csv_distinct.show()

## We also use OR/AND operator.
df_csv_OR_OPERATOR=df_select.where("deptno=12 OR deptlocation='Agra'" )
df_csv_OR_OPERATOR.sort("deptno").show()

## We also use OR/AND operator.
df_csv_AND_OPERATOR=df_select.where("deptno=20 OR deptlocation='pune'" )
df_csv_AND_OPERATOR.sort("deptno").show()

## We also use IN operator.
df_csv_in_OPERATOR=df_select.where("deptno in (10,40)")
df_csv_in_OPERATOR.sort("deptno").show()

## We also use NOT IN operator.
df_csv_not_in_OPERATOR=df_select.where("deptno not in (10,40)")
df_csv_not_in_OPERATOR.sort("deptno").show()

#### to pull data from parquet file.
from pyspark.sql.functions import col
file_path_parqt="/FileStore/tables/MTcars.parquet"
df1_parqt=spark.read.parquet(file_path_parqt,header=True)
df1_parqt.limit(5).show()
df1_parqt.count()
#df1_filter=df1_parqt.filter("model='Datsun 710' OR model='Mazda RX4'")
df1_filter=df1_parqt.filter("hp in (175,110,93)")
df1_filter.sort("cyl").show()


