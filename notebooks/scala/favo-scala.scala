// Databricks notebook source
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class FavoDataTables {
  val url_orders = "https://favo-data-test.s3.amazonaws.com/TestFolder/orders.json"
  val url_products = "https://favo-data-test.s3.amazonaws.com/TestFolder/products.json"
  val url_buyers = "https://favo-data-test.s3.amazonaws.com/TestFolder/buyers.json"
  val url_sellers = "https://favo-data-test.s3.amazonaws.com/TestFolder/sellers.json"
  
  type TupleDataFrame = (DataFrame, DataFrame, DataFrame, DataFrame)
  
  def getDatafromUrl(url: String): DataFrame ={
    
    val data = scala.io.Source.fromURL(url).mkString
    val fixedData = if (url.split("/")(4) == "buyers.json") {
       data.replace(".","") } else { data }
    val dataResponse = fixedData.toString().stripLineEnd.split("\r\n").map(_.trim).toList
    val jsonRdd = spark.sparkContext.parallelize(dataResponse) 
    val jsonDf = spark.read.json(jsonRdd)
    
    return jsonDf
  }
  
  def getOrdersTable(): DataFrame ={
    val orders = getDatafromUrl(url_orders)
    val ordersDf = orders.withColumn("product", explode($"product"))
          .withColumn("sku",col("product.sku"))
          .withColumn("qt",col("product.qt"))
          .drop("product")
    
    return ordersDf
  }
  
  def getProductTable(): DataFrame ={
    return getDatafromUrl(url_products)
  }
  
  def getBuyerTable(): DataFrame ={
    val df = getDatafromUrl(url_buyers)
    val buyers = df.withColumnRenamed("start", "buyer_start")
    return buyers
  }
  
  def getSellersTable(): DataFrame ={
    val sellers = getDatafromUrl(url_sellers)
              .withColumnRenamed("start", "seller_start")
    return sellers
  }
  
  def getAllTables(): TupleDataFrame ={
    return ( getOrdersTable(), getProductTable(), getBuyerTable(), getSellersTable() )
  }
}

// COMMAND ----------

val favo = new FavoDataTables
val (orders, products, buyers, sellers) = favo.getAllTables()

// COMMAND ----------

// DBTITLE 1,Solution 1
/*
Funtion that solve Question1 of test, with this function intance the FavoDataTables and get all tables from the four different Favo json files and concatenate all of them.
*/
def getSolutionOne(): DataFrame={
  val favo = new FavoDataTables
  val (orders, products, buyers, sellers) = favo.getAllTables()
  val full_table = orders.join(sellers, Seq("seller_id"))
              .join(buyers, Seq("buyer_id"), "left")
              .join(products, Seq("sku"))
              .drop($"orders.seller_id")
              .drop($"buyers.buyer_id")
              .drop($"products.sku")
  return full_table
}

getSolutionOne()

// COMMAND ----------

/*
This function use date function in order to formatted date and to get year and week of date column after that usa lag function in order to get the difference between  weeks in order to know the increase of sales.
*/
def getSolutionTwo(): DataFrame={
  val fullTable = getSolutionOne()
  val tempTable = fullTable.select($"seller_id",$"sku",$"qt",$"price",
                                      to_date(from_unixtime($"date")).alias("date"),
                                      to_date(from_unixtime($"seller_start")).alias("seller_start"))    
                                .withColumn("order_price", $"qt"*$"price")
                                .withColumn("year", year($"date"))
                                .withColumn("week", weekofyear($"date"))
                                .groupBy("year","week")
                                .agg(sum($"order_price").alias("week_sales"))
                                .sort(asc("year"),asc("week"))
  
  val window = Window.partitionBy("year").orderBy("week")
  val diffTable = tempTable.withColumn("sales_increase_by_week", $"week_sales" - lag("week_sales",1).over(window))
 
  return diffTable
}

val df = getSolutionTwo()
df.show()


// COMMAND ----------

// DBTITLE 1,Solution 3
/*
This function get at first the total sales of all dataset and base on that check the percentage of contribution in sales of the sellers.
*/
def getSolutionThree(): DataFrame = {
  val full_table = getSolutionOne()
  val total_sales = full_table.withColumn("total_price",col("price")*col("qt"))
    .agg(sum("total_price")).collect()(0)
  println(f"Total Sales: $total_sales")

  val DFOut = full_table.withColumn("total_price_perc",round((col("price") * col("qt") * 100) / total_sales(0),2))
    .groupBy("seller_id")
    .agg(sum("total_price_perc").alias("total_price_perc"))
    .sort(desc("total_price_perc"))
  
  return DFOut
}

val df = getSolutionThree()
df.show()

// COMMAND ----------

/*
This function at first get all SKUs sold with an empty buyer_id in order. Once we have that we filter this SKUs along all the order history in order to get the buyer that bought more qty  of that SKU, It is more likely that those buyers who buy those products in history, will buy more, that is why we filter and obtain the buyers who have bought a greater quantity of those SKUs.
*/
def getSolutionFour(): DataFrame ={
  val full_table = getSolutionOne()
  val BuyerNullValues = full_table.select($"sku").where($"buyer_id".isNull).collect().toList
  val SkuList = for (RowValue <- BuyerNullValues) yield RowValue(0)

  val filtered_table = full_table.where($"sku".isin(SkuList:_*))
    .where($"buyer_id".isNotNull)
    .groupBy(col("buyer_id"),col("sku"))
    .agg(
        countDistinct(col("order_id")).alias("times_buyed_prd"),
        sum(col("qt")).alias("qty_buyed_prd"))

  val FinalTable = filtered_table.select($"buyer_id",$"sku",$"qty_buyed_prd",
                        row_number().over(Window.partitionBy($"sku").orderBy($"qty_buyed_prd".desc)).alias("rowNum"))
              .filter($"rowNum" === 1)
  return FinalTable
}

val df = getSolutionFour()
df.show()
