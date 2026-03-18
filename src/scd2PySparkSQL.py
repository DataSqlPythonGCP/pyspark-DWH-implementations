# ------------------------------------------------------------------------
# This PySpark script is meant for performing SCD2 on Customer Dimension.
# Using SQL syntax.
# ------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("scd2PySparkSQL").getOrCreate()

# Load snapshot data of Jan 24 in data frame.
# This data will be used to perform full load for the first time in customer dimension.

snapshot_df = spark.read.csv('/Users/ad/Downloads/customer_snapshot_jan24.csv', header=True, inferSchema=True)
snapshot_df.printSchema()
print("\n Snapshot data sample.\n")
snapshot_df.show(2, truncate=False)

# Create a dimension table using the snapshot data.
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

customer_dim = snapshot_df.withColumnsRenamed(renamed_column)
customer_dim = customer_dim.withColumn('valid_from', lit('2024-01-01').cast('date')) \
    .withColumn('valid_to', to_date(lit("9999-12-31"))) \
    .withColumn('is_New', lit(True)) \
    .withColumn('customer_sk', monotonically_increasing_id())

customer_dim.show(2, truncate=False)
customer_dim = customer_dim.createOrReplaceTempView("customer_dim")

# Load snapshot feb 24 data in data frame.
# This is similar to snapshot feeds generated for subsequent dimension loads.
# ----------------------------------------------------------------------------

snapshot_feb = spark.read.csv('/Users/ad/Downloads/customer_snapshot_feb24.csv', header=True, inferSchema=True)
print("Staging data, the next snapshot after customer dimension load.")
snapshot_feb.show(2, truncate=False)
snapshot_feb.createOrReplaceTempView("snapshot_feb")

# Classify data such as 'New', 'Changed' or 'Unchanged'
# New : Insert, Changed : Upsert, Unchanged : Load as it is.
# ----------------------------------------------------------

classified_df = spark.sql('''
select 
s.*,
d.*,
case when d.customer_key is null then 'New'
when s.c_address <> d.customer_address or s.c_phone <> d.customer_phone or s.c_acctbal <> d.customer_account_balance then 'Changed'
else 'Unchanged' end as changed_record
from snapshot_feb s left join customer_dim d
on s.c_custkey = d.customer_key
and d.is_New = True
''')

print("\n After data classification.\n")
classified_df.show(2, truncate=False)

classified_df.createOrReplaceTempView("classified_df")

# Expire the Changed records

expired_df = spark.sql('''
select 
c.customer_sk,
c.customer_key,
c.customer_name,
c.customer_address,
c.customer_nationkey,
c.customer_phone,
c.customer_account_balance,
c.customer_market_segment,
c.customer_comment,
valid_from,
date_sub (current_date(),1) as valid_to,
False as is_New

from classified_df c
where
changed_record = 'Changed'
''')

expired_df.show(2, truncate=False)
expired_df.createOrReplaceTempView('expired_df')

# Insert New/Changed records

insert_new_records = spark.sql('''
select 
coalesce((select MAX(customer_sk) from customer_dim),0)
+ ROW_NUMBER() over( order by customer_key) as customer_sk,
c.c_custkey as customer_key,
c.c_name as customer_name,
c.c_address as customer_address,
c.c_nationkey as customer_nationkey,
c.c_phone as customer_phone,
c.c_acctbal as customer_account_balance,
c.c_mktsegment  as customer_market_segment,
c.c_comment as customer_comment,
current_date() as valid_from, 
CAST('9999-12-31' AS DATE) as valid_to,
True as is_New
from 
classified_df c
where c.changed_record in ('New', 'Changed')
''')

insert_new_records.show(2, truncate=False)
insert_new_records.createOrReplaceTempView("insert_new_records")

# Unchanged data should be inserted as is.

unchanged_df = spark.sql('''
select 
* from 
customer_dim c
where 
not exists ( select customer_key from classified_df cd where changed_record = 'Changed'
and c.customer_key = cd.c_custkey)
''')

unchanged_df.show(2, truncate=False)
unchanged_df.createOrReplaceTempView("unchanged_df")

# Combining all the data unchanged, changed and new into a final dimension.

final_customer_dim = spark.sql('''
select
customer_sk,
customer_key,
customer_name,
customer_address,
customer_nationkey,
customer_phone,
customer_account_balance,
customer_market_segment,
customer_comment,
valid_from,
valid_to,
is_New
from expired_df

union all 

select
customer_sk,
customer_key,
customer_name,
customer_address,
customer_nationkey,
customer_phone,
customer_account_balance,
customer_market_segment,
customer_comment,
valid_from,
valid_to,
is_New
from unchanged_df

union all 

select
customer_sk,
customer_key,
customer_name,
customer_address,
customer_nationkey,
customer_phone,
customer_account_balance,
customer_market_segment,
customer_comment,
valid_from,
valid_to,
is_New
from insert_new_records

''')
print("\n The final dimension.\n")
final_customer_dim.show(10, truncate=False)

#output_folder_path = "/Users/ad/PycharmProjects/Customer_Dimension_SCD2/data/dimension_output/"

# Write the DataFrame to a folder
#final_dimension.write.option("header", "true").mode("overwrite").csv(output_folder_path)

