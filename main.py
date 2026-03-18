from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Testing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = [
    {"id": 1, "name": "TestName1"},
    {"id": 2, "name": "TestName2"}, ]

schema = StructType([StructField ('id', IntegerType()),
                    StructField ( 'name', StringType())])

df = spark.createDataFrame(data, schema)
df.printSchema()
df.show(truncate=False)