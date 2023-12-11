// Databricks notebook source
// MAGIC %md
// MAGIC # Large File Count Results
// MAGIC
// MAGIC The large file count test compares metadata processing strategies across Lakehouses. We break the store_sales TPC-DS table up into 10MB files and experiment with 1,000 through 200,000 files. 
// MAGIC
// MAGIC #### This notebook creates store_sales table with the desired number of files
// MAGIC
// MAGIC
// MAGIC Notebook ported from https://github.com/lhbench/lhbench/blob/main/src/main/scala/benchmark/FileCountBenchmark.scala
// MAGIC

// COMMAND ----------

// DBTITLE 1,Variables to change
val database = "file_count_benchmark_delta_1k"
val tableLocation = s"s3://tpcds-results-test/lhbench/databases/${database}/store_sales"
val numFiles = 1000 // make numFiles a multiple of batchSize 
val batchSize = 1000

// COMMAND ----------

val fullTableName = s"$database.store_sales"

// spark.sql(s"create database if not exists $database")
// spark.sql(s"drop table if exists $fullTableName")
// dbutils.fs.rm(tableLocation, true)

// COMMAND ----------

import org.apache.spark.sql.functions._
import spark.implicits._
    

val count = if (numFiles > batchSize) numFiles/batchSize else 1
val numRecordsPerFile = 166666L  // create 10MB files
val numKeys = batchSize * numRecordsPerFile

for( i <- 1 to count){
  
  spark.range(0, numKeys, 1, batchSize)
    .withColumn("ss_item_sk", $"id")
    .withColumn("ss_sold_date_sk",  ($"id" / numRecordsPerFile).cast("bigint")) // partition column
    .withColumn("ss_sold_time_sk", $"id" * 23 + 7)
    .withColumn("ss_customer_sk", $"id" % 123)
    .withColumn("ss_cdemo_sk", $"id" % 37)
    .withColumn("ss_hdemo_sk", $"id" % 51)
    .withColumn("ss_addr_sk", $"id" % 117)
    .withColumn("ss_store_sk", $"id" % 13)
    .withColumn("ss_promo_sk", $"id" % 511)
    .withColumn("ss_quantity", $"id" % 7)
    .withColumn("ss_ticket_number", ($"id" * 91 + 23).cast("bigint"))
    .withColumn("ss_quantity", $"id" % 7)
    .withColumn("ss_wholesale_cost", (randn() * 1000).cast("decimal(7,2)"))
    .withColumn("ss_list_price", (randn() * 2000).cast("decimal(7,2)"))
    .withColumn("ss_wholesale_cost", (randn() * 3000).cast("decimal(7,2)"))
    .withColumn("ss_sales_price", (randn() * 4000).cast("decimal(7,2)"))
    .withColumn("ss_ext_discount_amt", (randn() * 5000).cast("decimal(7,2)"))
    .withColumn("ss_ext_sales_price", (randn() * 6000).cast("decimal(7,2)"))
    .withColumn("ss_ext_wholesale_cost", (randn() * 7000).cast("decimal(7,2)"))
    .withColumn("ss_ext_list_price", (randn() * 8000).cast("decimal(7,2)"))
    .withColumn("ss_ext_tax", (randn() * 9000).cast("decimal(7,2)"))
    .withColumn("ss_coupon_amt", (randn() * 10000).cast("decimal(7,2)"))
    .withColumn("ss_net_paid", (randn() * 11000).cast("decimal(7,2)"))
    .withColumn("ss_net_paid_inc_tax", (randn() * 12000).cast("decimal(7,2)"))
    .withColumn("ss_net_profit", (randn() * 13000).cast("decimal(7,2)"))
    .drop("id")
    .write
    .format("delta")
    .mode("append")
    .partitionBy("ss_sold_date_sk")
    .option("path", tableLocation)
    .saveAsTable(fullTableName)    
  
}


// COMMAND ----------

display(spark.sql(s"DESCRIBE DETAIL `delta`.`$tableLocation`"))

// COMMAND ----------


