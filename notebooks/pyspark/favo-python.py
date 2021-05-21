# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql.types import DateType
from urllib.request import urlopen
import json

# COMMAND ----------

class FavoDataTables():
  
  def __init__(self, spark):
    self.spark = spark
    self.url_orders = "https://favo-data-test.s3.amazonaws.com/TestFolder/orders.json"
    self.url_products = "https://favo-data-test.s3.amazonaws.com/TestFolder/products.json"
    self.url_buyers = "https://favo-data-test.s3.amazonaws.com/TestFolder/buyers.json"
    self.url_sellers = "https://favo-data-test.s3.amazonaws.com/TestFolder/sellers.json"
    
  def getDatafromUrl(self, url):
    data = urlopen(url).read().decode('utf-8')
    #Fix for phone number coming with '.' in buyers file
    data = data.replace(".","") if url.split("/")[4] == "buyers.json" else data
    jsonData = [json.loads(line) for line in data.replace(" ","").split("\r\n") if line != '']
    rdd = self.spark.sparkContext.parallelize(jsonData)
    df = self.spark.read.json(rdd)

    return df
  
  def getOrdersTable(self):
    orders = self.getDatafromUrl(self.url_orders)
    orders = orders.withColumn("product", explode("product"))\
        .withColumn("sku",col("product.sku"))\
        .withColumn("qt",col("product.qt"))\
        .drop("product")
    
    return orders
  
  def getProductTable(self):
    return self.getDatafromUrl(self.url_products)

  def getBuyerTable(self):
    buyers = self.getDatafromUrl(self.url_buyers)\
              .withColumnRenamed('start', 'buyer_start')
    return buyers
  
  def getSellersTable(self):
    sellers = self.getDatafromUrl(self.url_sellers)\
              .withColumnRenamed('start', 'seller_start')
    return sellers
  
  def getAllTables(self):
    return self.getOrdersTable(), self.getProductTable(), self.getBuyerTable(), self.getSellersTable()

# COMMAND ----------

# DBTITLE 1,Solution 1
def getSolutionOne(spark):
  """Funtion that solve Question1 of test, with this function intance the FavoClass and get all tables from the four different Favo json files and concatenate all of them.
  :param: spark -> SparkSession.
  """
  favo_data = FavoDataTables(spark)
  orders, products, buyers, sellers = favo_data.getAllTables()
  full_table = orders.alias("a").join(sellers, orders.seller_id == sellers.seller_id)\
              .join(buyers.alias("b"), orders.buyer_id == buyers.buyer_id, how = 'left')\
              .join(products.alias("c"), orders.sku == products.sku)\
              .drop(orders.seller_id)\
              .drop(buyers.buyer_id)\
              .drop(products.sku)
  return full_table

getSolutionOne(spark)

# COMMAND ----------

# DBTITLE 1,Solution 2
def getSolutionTwo(spark):
  """This function use date function in order to formatted date and to get year and week of date column after that usa lag function in order to get the difference between  weeks in order to know the increase of sales.
  :param: spark -> SparkSession.
  """
  full_table = getSolutionOne(spark)

  temp_table = full_table.select('seller_id','sku','qt','price',
                                 from_unixtime('date').cast(DateType()).alias('date'),
                                 from_unixtime('seller_start').cast(DateType()).alias('seller_start'))\
                    .withColumn('order_price',col('qt')*col('price'))\
                    .withColumn('year', year('date'))\
                    .withColumn("week", weekofyear("date"))\
                    .groupBy('year','week')\
                    .agg(sum('order_price').alias('week_sales'))\
                    .orderBy(col('year'), col('week'))

  window = Window.partitionBy("year").orderBy("week")
  temp_table.withColumn("sales_increase_by_week", col("week_sales") - lag(col("week_sales")).over(window))\
            .show()

getSolutionTwo(spark)

# COMMAND ----------

# DBTITLE 1,Solution 3
def getSolutionThree(spark):
  """This function get at first the total sales of all dataset and base on that check the percentage of contribution in sales of the sellers.
  :param: spark -> SparkSession.
  """
  full_table = getSolutionOne(spark)
  total_sales = full_table.withColumn("total_price",col("price")*col("qt"))\
    .agg(sum("total_price")).collect()[0]
  print(f"Total Sales: {total_sales[0]}")

  full_table.withColumn("total_price_perc",round(100*(col("price")*col("qt"))/total_sales[0],2))\
    .groupBy("seller_id")\
    .agg(sum("total_price_perc").alias("total_price_perc"))\
    .orderBy(col("total_price_perc").desc())\
    .show()

getSolutionThree(spark)

# COMMAND ----------

# DBTITLE 1,Solution 4
def getSolutionFour(spark):
  """This function at first get all SKUs sold with an empty buyer_id in order. Once we have that we filter this SKUs along all the order history in order to get the buyer that bought more qty  of that SKU, It is more likely that those buyers who buy those products in history, will buy more, that is why we filter and obtain the buyers who have bought a greater quantity of those SKUs.
  :param: spark -> SparkSession.
  """
  full_table = getSolutionOne(spark)
  sku_list = [int(row.sku)for row in full_table.select(col('sku')).where(col('buyer_id').isNull()).collect()]
  print(f"Sku with empty buyer_id info: {sku_list}")
  filtered_table = full_table.where(col('sku').isin(sku_list))\
    .where(col('buyer_id').isNotNull())\
    .groupBy(col('buyer_id'),col('sku'))\
    .agg(
        countDistinct(col('order_id')).alias('times_buyed_prd'),
        sum(col('qt')).alias('qty_buyed_prd'))

  filtered_table.select('buyer_id','sku','qty_buyed_prd',
                        row_number().over(Window.partitionBy("sku").orderBy(col("qty_buyed_prd").desc())).alias("rowNum"))\
              .where(col('rowNum')==1).show()

getSolutionFour(spark)
