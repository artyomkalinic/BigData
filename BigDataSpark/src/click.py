from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = (SparkSession.builder.appName("ReportsToClickHouse").config("spark.jars", "/opt/jars/postgresql.jar," "/opt/jars/clickhouse.jar").getOrCreate())

postgres_url = "jdbc:postgresql://postgres:5432/db"
postgres_properties = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

click_url = "jdbc:clickhouse://clickhouse:8123/default"
click_driver = "com.clickhouse.jdbc.ClickHouseDriver"

sales = spark.read.jdbc(url=postgres_url, table="sales", properties=postgres_properties)
prod = spark.read.jdbc(url=postgres_url, table="product",  properties=postgres_properties)
cust = spark.read.jdbc(url=postgres_url, table="customer", properties=postgres_properties)
date = spark.read.jdbc(url=postgres_url, table="date", properties=postgres_properties)
stor = spark.read.jdbc(url=postgres_url, table="store",  properties=postgres_properties)
supp = spark.read.jdbc(url=postgres_url, table="supplier", properties=postgres_properties)


#Витрина продаж по поставщикам Цель: Анализ эффективности поставщиков

#Топ-5 поставщиков с наибольшей выручкой
top5_suppliers = (
    sales
    .join(prod.select("product_sk", "supplier_id"), "product_sk")
    .join(supp, supp.supplier_sk == F.col("supplier_id"))
    .groupBy("name", "city", "country")
    .agg(F.sum("total_price").alias("revenue"))
    .orderBy(F.desc("revenue"))
    .limit(5)
)
top5_suppliers.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "top5_suppliers").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()

# Средняя цена товаров от каждого поставщика
avg_price_by_supplier = (
    sales
    .join(prod.select("product_sk", "supplier_id", F.col("price").alias("product_price")), "product_sk").join(supp.select("supplier_sk", "name"), supp.supplier_sk == F.col("supplier_id"))
    .groupBy("name")
    .agg(F.avg("product_price").alias("avg_price"))
    .select("name", "avg_price")
)
avg_price_by_supplier.write.format("jdbc").mode("overwrite").option("url",     click_url).option("dbtable", "avg_price_by_supplier").option("driver",  click_driver).option("createTableOptions", "ENGINE = Log").save()

# Распределение продаж по странам поставщиков
sales_by_supplier_country = (
    sales.join(prod.select("product_sk", "supplier_id"), "product_sk").join(supp, supp.supplier_sk == F.col("supplier_id"))
    .groupBy("country")
    .agg(F.sum("total_price").alias("revenue"), F.sum("quantity").alias("quantity"))
    .orderBy(F.desc("revenue"))
)
sales_by_supplier_country.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "sales_by_supplier_country").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()





#1 Витрина продаж по продуктам. Цель: Анализ выручки, количества продаж и популярности продуктов

# Топ-10 самых продаваемых продуктов
top10_prods = (
    sales.groupBy("product_sk").agg(F.sum("quantity").alias("total_quantity"), F.sum("total_price").alias("total_revenue"))
        .join(prod, "product_sk")
        .select("product_id","name","category","total_quantity","total_revenue")
        .orderBy(F.desc("total_quantity"))
        .limit(10)
)

top10_prods.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "top10_products").option("driver",  click_driver).option("user", "default").option("createTableOptions", "ENGINE = Log").save()

# Общая выручка по категориям продуктов
total_by_category = (
    sales.join(prod, "product_sk")
        .groupBy("category")
        .agg(F.sum("total_price").alias("total_revenue"))
        .orderBy(F.desc("total_revenue"))
)
total_by_category.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "revenue_by_category").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()

# Средний рейтинг и количество отзывов для каждого продукта
prod_rate = prod.select("product_id","name","category","rating","reviews")
prod_rate.write.format("jdbc").mode("overwrite").option("url",     click_url).option("dbtable", "product_ratings").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()




#2 Витрина продаж по клиентам Цель: Анализ покупательского поведения и сегментация клиентов.

# Топ-10 клиентов с наибольшей общей суммой покупок  
top10_customers = (
    sales.groupBy("customer_sk")
        .agg(F.sum("total_price").alias("total_spent"))
        .join(cust, "customer_sk")
        .select("customer_id","first_name","last_name","country","total_spent")
        .orderBy(F.desc("total_spent"))
        .limit(10)
)
top10_customers.write.format("jdbc").mode("overwrite").option("url",     click_url).option("dbtable", "top10_customers").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()

#Распределение клиентов по странам
customers_by_country = (
    cust.groupBy("country")
         .agg(F.countDistinct("customer_id").alias("num_customers"))
         .orderBy(F.desc("num_customers"))
)

customers_by_country.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "customers_by_country").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()

# Средний чек для каждого клиентa
avg_check_by_customer = (
    sales.groupBy("customer_sk")
        .agg((F.sum("total_price") / F.count("quantity")).alias("avg_check"))
        .join(cust, "customer_sk")
        .select("customer_id","avg_check")
)
avg_check_by_customer.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "avg_check_by_customer").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()




#3 Витрина продаж по клиентам Цель: Анализ покупательского поведения и сегментация клиентов
# Месячные и годовые тренды продаж
month = (
    sales.join(date, "date_sk")
        .groupBy("year","month")
        .agg(
            F.sum("total_price").alias("revenue"),
            F.sum("quantity").alias("quantity")
        )
        .orderBy("year","month")
)
month.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "monthly_trends").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()

year = (
    sales.join(date, "date_sk")
        .groupBy("year")
        .agg(F.sum("total_price").alias("revenue"), F.sum("quantity").alias("quantity"))
        .orderBy("year")
)

year.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "yearly_trends").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()

#Сравнение выручки за разные периоды
diff = (
    month.withColumn("prev_year_revenue", F.lag("revenue").over(Window.partitionBy("month").orderBy("year")))
      .na.fill({"prev_year_revenue": 0.0})
      .withColumn(
            "diff_change",
            (F.col("revenue") - F.col("prev_year_revenue")) / F.col("prev_year_revenue")
      )
      .na.fill({"diff_change": 0.0})
      .select("year", "month", "revenue", "prev_year_revenue", "diff_change")
)
diff.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "yoy_trends_by_month").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()


# Средний размер заказа по месяцам
avg_order_size = (month.withColumn("avg_order_size", F.col("revenue") / F.col("quantity")).select("year","month","avg_order_size"))
avg_order_size.write.format("jdbc").mode("overwrite").option("url",click_url).option("dbtable", "avg_order_size_by_month").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()




#4 Витрина продаж по магазинам Цель: Анализ эффективности магазинов

#Топ-5 магазинов с наибольшей выручкой
top5_stores = (
    sales.groupBy("store_sk")
        .agg(F.sum("total_price").alias("revenue"))
        .join(stor, "store_sk")
        .select("name","city","country","revenue")
        .orderBy(F.desc("revenue"))
        .limit(5)
)
top5_stores.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "top5_stores").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()

#Распределение продаж по городам и странам
sales_by_city_country = (
    sales.join(stor, "store_sk")
        .groupBy("city","country")
        .agg(F.sum("total_price").alias("revenue"), F.sum("quantity").alias("quantity"))
        .orderBy(F.desc("revenue"))
)
sales_by_city_country.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "sales_by_city_country").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()

# Средний чек для каждого магазина
avg_check_by_store = (
    sales.groupBy("store_sk")
        .agg((F.sum("total_price") / F.count("quantity")).alias("avg_check"))
        .join(stor, "store_sk")
        .select("name","avg_check")
)
avg_check_by_store.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "avg_check_by_store").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()




#6 Витрина качества продукции Цель: Анализ отзывов и рейтингов товаров

#Продукты с наивысшим и наименьшим рейтингом
highest_rated = prod.orderBy(F.desc("rating")).limit(10).select("product_id","name","rating")

highest_rated.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "highest_rated_products").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()

lowest_rated = prod.orderBy(F.asc("rating")).limit(10).select("product_id","name","rating")
lowest_rated.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "lowest_rated_products").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()

# Корреляция между рейтингом и объемом продаж
rate_sales = (
    sales.join(prod, "product_sk")
        .groupBy("product_id","name")
        .agg(F.avg("rating").alias("avg_rating"),F.sum("quantity").alias("total_quantity"))
)
corr_value = rate_sales.stat.corr("avg_rating", "total_quantity")
corr_df = spark.createDataFrame([(corr_value,)], ["rating_sales_correlation"])
corr_df.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "rating_sales_correlation").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()

#Продукты с наибольшим количеством отзывов
top_reviewed = prod.orderBy(F.desc("reviews")).limit(10).select("product_id","name","reviews")
top_reviewed.write.format("jdbc").mode("overwrite").option("url", click_url).option("dbtable", "top_reviewed_products").option("driver",  click_driver).option("user", "default").option("password", "").option("createTableOptions", "ENGINE = Log").save()

spark.stop()