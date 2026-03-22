from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime

spark = SparkSession.builder.appName("FactDimensionLoad").getOrCreate()

# Load customer dimension from csv
# ----------------------------------
customer_df = spark.read.csv("/Users/ad/Downloads/customer_dim.csv",header=True,inferSchema=True)
#customer_df.printSchema()
print("\n Customer Dimension Table.\n")
customer_df.show(truncate=False)
customer_df.createOrReplaceTempView("customer_dim")


# Load product dimension from csv
# -------------------------------
product_df = spark.read.csv("/Users/ad/Downloads/product_dim.csv",header=True,inferSchema=True)
#product_df.printSchema()
print("\n Product Dimension Table.\n")
product_df.show(truncate=False)
product_df.createOrReplaceTempView("product_dim")

# Load raw orders data from csv
# ------------------------------
raw_order_df = spark.read.csv("/Users/ad/Downloads/order_details_LADim.csv",header=True,inferSchema=True)
#raw_order_df.printSchema()
raw_order_df.show(truncate=False)
raw_order_df.createOrReplaceTempView("raw_order")

# Load the date dimension 2025 - 2026
# -----------------------------------
def generate_date_dimension(start_date="2025-01-01", end_date="2026-12-31"):
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    date_data = []
    date_sk = 1

    current = start
    while current <= end:
        date_data.append((
            date_sk,
            current.strftime("%Y-%m-%d"),
            int(current.strftime("%Y")),
            int(current.strftime("%m")),
            current.strftime("%B"),
            int(current.strftime("%d")),
            current.strftime("%A"),
            current.weekday() + 1,  # Monday=1, Sunday=7
            1 if current.weekday() < 5 else 0,  # Is weekday
            1 if current.weekday() in [5, 6] else 0  # Is weekend
        ))
        current += datetime.timedelta(days=1)
        date_sk += 1

    return date_data


date_data = generate_date_dimension()
date_schema = StructType([
    StructField("date_sk", IntegerType(), False),
    StructField("full_date", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("month_name", StringType(), True),
    StructField("day", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("is_weekday", IntegerType(), True),
    StructField("is_weekend", IntegerType(), True)
])

df_date = spark.createDataFrame(date_data, schema=date_schema)
df_date.show(5, truncate=False)
print("\n Date Dimension Table.\n")
df_date.createOrReplaceTempView("date_dim")

# Creating a star schema with orders fact and customer/product/date dimension.
# ----------------------------------------------------------------------------
fact_orders_df = spark.sql('''select
r.order_id,
r.customer_id,
r.product_id,
case when c.customer_sk is not null then c.customer_sk else '-777' end as customer_sk, 
case when p.product_sk is not null then p.product_sk else '-999' end as product_sk, 
d.date_sk,
r.quantity, 
r.unit_price, 
r.discount,
r.status
from 
raw_order r left join customer_dim c
on 
r.customer_id = c.customer_id
left join product_dim p
on 
r.product_id = p.product_id
left join date_dim d
on
r.date_id = d.full_date
where 
(c.is_active = True or c.is_active is null)
and
(p.is_active = True or p.is_active is null)
''')

print("\n Fact Orders Table (Star Schema).\n")
print("\n Process the fact records with dummy dimension keys, so that revenue calculations are not impacted.\n")
fact_orders_df.show(truncate=False)
fact_orders_df.createOrReplaceTempView("fact_orders")

suspended_fact_orders_df = spark.sql('''
select 
o.*, 
case 
    when o.customer_sk = '-777' or o.product_sk = '-999' then False 
    else True end as processed_flag 
from fact_orders o
where
o.customer_sk = '-777' or o.product_sk = '-999' 
''')
print("\nThe suspended fact table would look like below.\n")
suspended_fact_orders_df.show(truncate=False)
suspended_fact_orders_df.createOrReplaceTempView("suspended_fact_orders")

# Load new raw orders data from csv
# ---------------------------------
raw_order_df = spark.read.csv("/Users/ad/Downloads/New_Order_Details.csv",header=True,inferSchema=True)
#raw_order_df.printSchema()
print("\n New orders/transactions after initial load.\n")
raw_order_df.show(truncate=False)
raw_order_df.createOrReplaceTempView("new_raw_orders")

# Load customer dimension from csv
# ----------------------------------
customer_df = spark.read.csv("/Users/ad/Downloads/updt_customer_dim.csv",header=True,inferSchema=True)
#customer_df.printSchema()
print("\n Updated Customer Dimension Table.\n")
customer_df.show(truncate=False)
customer_df.createOrReplaceTempView("updt_customer_dim")


# Load product dimension from csv
# -------------------------------
product_df = spark.read.csv("/Users/ad/Downloads/updt_product_dim.csv",header=True,inferSchema=True)
#product_df.printSchema()
print("\n Updated Product Dimension Table.\n")
product_df.show(truncate=False)
product_df.createOrReplaceTempView("updt_product_dim")


# The fact load in next schedule or when next set of orders are processed.
# The suspended facts are also included to check if relevant dimensions are available or not.
#------------------------------------------------------------------------------------------
suspended_fact_order = spark.sql('''select
sf.order_id,
sf.customer_id,
sf.product_id,
c.customer_sk,
p.product_sk,
sf.date_sk,
sf.quantity,
sf.unit_price,
sf.discount,
sf.status, 
case 
    when c.customer_sk is null or p.product_sk is null then False 
    else True end as processed_flag 
from suspended_fact_orders sf 

left join updt_customer_dim c
on 
sf.customer_id = c.customer_id

left join updt_product_dim p
on 
sf.product_id = p.product_id
where
sf.processed_flag = False
and
(c.is_active = True or c.is_active is null)
and
(p.is_active = True or p.is_active is null)
''')

print("Suspended fact tables must be updated after dimensions are available, notice processed flag.\n")
suspended_fact_order.show(truncate=False)
suspended_fact_order.createOrReplaceTempView("suspended_fact_order")

fact_order_df = spark.sql('''
select
r.order_id,
r.customer_id,
r.product_id,
c.customer_sk, 
p.product_sk, 
d.date_sk,
r.quantity, 
r.unit_price, 
r.discount,
r.status
from 
new_raw_orders r left join updt_customer_dim c
on 
r.customer_id = c.customer_id
left join updt_product_dim p
on 
r.product_id = p.product_id
left join date_dim d
on
r.date_id = d.full_date
where 
(c.is_active = True or c.is_active is null)
and
(p.is_active = True or p.is_active is null)

union 

select 
f.order_id,
f.customer_id,
f.product_id,
case when f.customer_sk = '-777' then sf.customer_sk else f.customer_sk end customer_sk, 
case when f.product_sk = '-999' then sf.product_sk else f.product_sk end product_sk, 
f.date_sk,
f.quantity, 
f.unit_price, 
f.discount,
f.status
from
fact_orders f left join suspended_fact_order sf
on 
f.order_id = sf.order_id
order by order_id
''')

print("\n The fact table would like below :\n")
fact_order_df.show(25,truncate=False)
fact_order_df.createOrReplaceTempView("new_fact_orders")
