from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, expr
from pyspark.sql.window import Window

spark = (SparkSession.builder.appName("Snow").config("spark.jars", "/opt/jars/postgresql.jar").getOrCreate())

postgres_url = "jdbc:postgresql://postgres:5432/db"
postgres_properties = {"user":"postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

all = (spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "mock_data").option("user", postgres_properties["user"]).option("password",postgres_properties["password"]).option("driver", postgres_properties["driver"]).load())

def into_table(df, part_column, order_column, selects, new_name, target_table):
    win = Window.partitionBy(part_column).orderBy(order_column)
    df_dim = (df.select(part_column, order_column, *selects).withColumn("rn", row_number().over(win)).filter(col("rn") == 1).drop("rn", order_column))
    
    for old, new in new_name.items():
        df_dim = df_dim.withColumnRenamed(old, new)

    df_dim.write.mode("append").jdbc(postgres_url, target_table, properties=postgres_properties)

into_table(all, part_column="sale_customer_id", order_column="sale_date",
    
    selects=["customer_first_name", "customer_last_name", "customer_age", "customer_email", "customer_country", "customer_postal_code"],
    
    new_name={"sale_customer_id": "customer_id", "customer_first_name": "first_name", "customer_last_name": "last_name",
        "customer_age": "age", "customer_email": "email", "customer_country": "country", "customer_postal_code": "postal_code"},
    
    target_table="customer"
)

into_table(all, part_column="sale_seller_id", order_column="sale_date",
         
    selects=[ "seller_first_name", "seller_last_name", "seller_email", "seller_country", "seller_postal_code"],

    new_name={ "sale_seller_id": "seller_id", "seller_first_name": "first_name", "seller_last_name": "last_name",
        "seller_email": "email", "seller_country": "country", "seller_postal_code": "postal_code"},

    target_table="seller"
)

into_table(all,  part_column="sale_product_id", order_column="sale_date",
    selects=["product_name", "product_category", "product_price", "product_weight", "product_color", "product_size", "product_brand", "product_material", "product_description",
        "product_rating", "product_reviews", "product_release_date", "product_expiry_date", "pet_category"
    ],
    new_name={"sale_product_id": "product_id", "product_name": "name", "product_category": "category", "product_price": "price",
        "product_weight": "weight", "product_color": "color", "product_size": "size", "product_brand": "brand", 
        "product_material": "material", "product_description": "description", "product_rating": "rating",
        "product_reviews": "reviews", "product_release_date": "release_date", "product_expiry_date": "expiry_date", "pet_category": "pet_category"},
    
    target_table="product"
)

into_table(all, part_column="store_name", order_column="sale_date",
    selects=["store_location", "store_city", "store_state", "store_country", "store_phone", "store_email"],
    
    new_name={"store_name": "name", "store_location": "location", "store_city": "city", "store_state": "state",
        "store_country": "country", "store_phone": "phone", "store_email": "email"},
    
    target_table="store"
)

into_table(all, part_column="supplier_name", order_column="sale_date",
    selects=["supplier_contact", "supplier_email", "supplier_phone", "supplier_address", "supplier_city", "supplier_country"],

    new_name={"supplier_name": "name", "supplier_contact": "contact", "supplier_email": "email", "supplier_phone": "phone",
        "supplier_address": "address", "supplier_city": "city", "supplier_country": "country"},
    
    target_table="supplier"
)

dim_dates = (all.select("sale_date").distinct().withColumn("year", expr("year(sale_date)")).withColumn("quarter", expr("quarter(sale_date)")).withColumn("month",   expr("month(sale_date)")).withColumn("day", expr("day(sale_date)")).withColumn("weekday", expr("dayofweek(sale_date)")))

dim_dates.write.mode("append").jdbc(postgres_url, "date", properties=postgres_properties)

cust = spark.read.jdbc(postgres_url, "customer", properties=postgres_properties)
sell = spark.read.jdbc(postgres_url, "seller",   properties=postgres_properties)
prod = spark.read.jdbc(postgres_url, "product",  properties=postgres_properties)
stor = spark.read.jdbc(postgres_url, "store",    properties=postgres_properties)
supp = spark.read.jdbc(postgres_url, "supplier", properties=postgres_properties)
date   = spark.read.jdbc(postgres_url, "date",     properties=postgres_properties)

sales = (all.join(date, all.sale_date == date.sale_date).join(cust, all.sale_customer_id == cust.customer_id)
    .join(sell, all.sale_seller_id == sell.seller_id).join(prod, all.sale_product_id  == prod.product_id)
    .join(stor, all.store_name == stor.name).join(supp, all.supplier_name == supp.name)
    .select(date.date_sk.alias("date_sk"), cust.customer_sk.alias("customer_sk"), sell.seller_sk.alias("seller_sk"), prod.product_sk.alias("product_sk"),
        stor.store_sk.alias("store_sk"),
        col("sale_quantity").alias("quantity"),
        col("sale_total_price").alias("total_price"),
        col("product_price").alias("price") 
    )
)

sales.write.mode("append").jdbc(postgres_url, "sales", properties=postgres_properties)

spark.stop()