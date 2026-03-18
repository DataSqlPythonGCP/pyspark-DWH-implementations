# ---------------------------------------------------------------------------------
# This PySpark script is meant for creating customer dimension using snapshot data.
# The customers data file  was split into 3 snapshot files with some changes so that
# SCD2 can be implemented.
# ---------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_sub, current_date
from pyspark.sql.functions import lit, monotonically_increasing_id, col, when, to_date, max, window, row_number
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("PySparkSCD2").getOrCreate()

df = spark.read.csv('/Users/ad/Downloads/customer_snapshot_jan24.csv', header=True, inferSchema=True)

df.printSchema()
print("\n Original Snapshot Data.\n")
df.show(5, truncate=False)

# The schema inferred by pyspark is as below
# ____________________________________________
# root
#  |-- c_custkey: integer (nullable = true)
#  |-- c_name: string (nullable = true)
#  |-- c_address: string (nullable = true)
#  |-- c_nationkey: integer (nullable = true)
#  |-- c_phone: string (nullable = true)
#  |-- c_acctbal: double (nullable = true)
#  |-- c_mktsegment: string (nullable = true)
#  |-- c_comment: string (nullable = true)
#  |-- snapshot_date: date (nullable = true)

renamed_column = {
    'c_custkey': 'customer_key',
    'c_name': 'customer_name',
    'c_address': 'customer_address',
    'c_nationkey': 'customer_nationkey',
    'c_phone': 'customer_phone',
    'c_acctbal': 'customer_account_balance',
    'c_mktsegment': 'customer_market_segment',
    'c_comment': 'customer_comment'
}

customer_dim = df.withColumnsRenamed(renamed_column)
customer_dim = customer_dim.withColumn('valid_from', lit('2024-01-01').cast('date')) \
    .withColumn('valid_to', to_date(lit("9999-12-31"))) \
    .withColumn('is_New', lit(True)) \
    .withColumn('customer_sk', monotonically_increasing_id())

print("Customer Dimension created from original data.")
customer_dim.printSchema()
customer_dim.show(5, truncate=False)

staging_data_feb = spark.read.csv('/Users/ad/Downloads/customer_snapshot_feb24.csv', header=True, inferSchema=True)
print("Staging data, the next snapshot after customer dimension load.")
staging_data_feb.show(5, truncate=False)

# Left join Staging & Customer Dim to classify records.
# Changes could be address, phone, account balance in customer dimension.
# Based on the case we create a new column to identify Changed, Unchanged or New records.
# ---------------------------------------------------------------------------------------

joined_df = staging_data_feb.join(customer_dim, staging_data_feb.c_custkey == customer_dim.customer_key, how='left')
classified_df = joined_df.withColumn(
    'changed_record', when(col('customer_key').isNull(), 'New') \
        .when((col('c_address') != col('customer_address'))
              | (col('c_phone') != col('customer_phone'))
              | (col('c_acctbal') != col('customer_account_balance')), 'Changed') \
        .otherwise('Unchanged')
)
print("\n Classified records ")
classified_df.show(5, truncate=False)

# Expire the existing customer dimension records.
# -----------------------------------------------
prev_day = date_sub(current_date(), 1)

expired_df = classified_df \
    .withColumn('is_New', when(col('changed_record') == 'Changed', lit(False)).otherwise(col('is_New'))) \
    .withColumn('valid_to', when(col('changed_record') == 'Changed', prev_day).otherwise(col('valid_to')))
expired_df.show(5, truncate=False)

# Insert the new records
# Changed records insert
# New records insert
# -----------------------
print("\n New Records, only expired record will have false.\n")
classified_df.filter(col('is_New') == True).show(5, truncate=False)
print("\n New records to be loaded first time load.\n")
classified_df.filter(col('customer_key').isNull()).show(5, truncate=False)

# Get the max surrogate key existing in the customer dimension.
# Retain the existing SK's and create new SK's for New, Changed (New) records.
# ----------------------------------------------------------------------------
max_sk = customer_dim.agg(max("customer_sk")).collect()[0][0] or 0
print("The max sk in existing customer dimension\n", max_sk)
customer_dim_with_sk = (
    classified_df
    .withColumn("row_num", row_number().over(
        Window.partitionBy().orderBy("customer_key")
    ))
    .withColumn(
        "customer_sk",
        col("row_num") + lit(max_sk)  # continues from max existing SK
    )
    .drop("row_num")
)

# Insert New / Changed rows
# --------------------------
insert_rows = customer_dim_with_sk.filter(col('changed_record').isin("New", "Changed")) \
    .select(
    col('c_custkey').alias('customer_key'),
    col('c_name').alias('customer_name'),
    col('c_address').alias('customer_address'),
    col('c_nationkey').alias('customer_nationkey'),
    col('c_phone').alias('customer_phone'),
    col('c_acctbal').alias('customer_account_balance'),
    col('c_mktsegment').alias('customer_market_segment'),
    col('c_comment').alias('customer_comment'),
    col('customer_sk').alias('customer_sk')
)
insert_new_rows = insert_rows.withColumn('valid_to', to_date(lit("9999-12-31"))) \
    .withColumn('is_New', lit(True)) \
    .withColumn('valid_from', lit(current_date()))

insert_new_rows.show(5, truncate=False)

# Final column list as per customer dimension table.
#---------------------------------------------------
common_columns = ['customer_sk', 'customer_key', 'customer_name', 'customer_address',
                  'customer_nationkey', 'customer_phone', 'customer_account_balance',
                  'valid_from', 'valid_to', 'is_New']

# Final dimension data
#----------------------
final_dimension = (
    expired_df.select(common_columns).union(insert_new_rows.select(common_columns).orderBy('customer_key'))
)

final_dimension.show(5, truncate=False)
final_dimension.filter(col('customer_key') == 27).show(5, truncate=False)


#output_folder_path = "/Users/ad/PycharmProjects/Customer_Dimension_SCD2/data/dimension_output/"

# Write the DataFrame to a folder
#final_dimension.write.option("header", "true").mode("overwrite").csv(output_folder_path)

