from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime

spark = SparkSession.builder.appName("FactDimensionLoad").getOrCreate()

# Load customer dimension from csv
# ----------------------------------
customer_df = spark.read.csv("/Users/ad/Downloads/customer_dim.csv",header=True,inferSchema=True)
customer_df.printSchema()
print("\n Customer Dimension Table.\n")
customer_df.show(truncate=False)
customer_df.createOrReplaceTempView("customer_dim")


# Load product dimension from csv
# -------------------------------
product_df = spark.read.csv("/Users/ad/Downloads/product_dim.csv",header=True,inferSchema=True)
product_df.printSchema()
print("\n Product Dimension Table.\n")
product_df.show(truncate=False)
product_df.createOrReplaceTempView("product_dim")

# Load raw orders data from dsv
# ------------------------------
raw_order_df = spark.read.csv("/Users/ad/Downloads/order_details.csv",header=True,inferSchema=True)
raw_order_df.printSchema()
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
c.customer_sk, 
p.product_sk, 
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
c.is_active = True
and
p.is_active = True
''')

print("\n Fact Orders Table (Star Schema).\n")
fact_orders_df.show(5, truncate=False)
fact_orders_df.createOrReplaceTempView("fact_orders")

# Now perform some basic analysis on the star schema / model we just created

# Sales by Product Category
# --------------------------
print ("\n Sales by Product Category \n")
category_wise_sales = spark.sql('''
select
p.category product_category, 
sum (quantity) total_quantity,
sum (unit_price-discount) total_sales_amount,
count(distinct order_id) order_count
from 
fact_orders f join product_dim p 
on
f.product_sk = p.product_sk
and 
f.status = 'COMPLETED'
group by p.category 

''')
category_wise_sales.show(5, truncate=False)

# Highest revenue generating customers
# -------------------------------------
print ("\n Highest revenue generating customers.\n")

top_customers = spark.sql('''
select 
c.customer_sk,
c.customer_name customer_name,
count(f.order_id) order_count,
sum(f.unit_price-discount) total_sales_amount,
avg(f.unit_price-discount) average_sales_amount,
sum(f.quantity) total_quantity_purchased
from fact_orders f join customer_dim c 
on 
f.customer_sk = c.customer_sk
and
f.status = 'COMPLETED'
group by c.customer_sk, c.customer_name
order by total_sales_amount desc

''')

top_customers.show(5, truncate=False)