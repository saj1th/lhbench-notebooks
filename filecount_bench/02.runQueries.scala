// Databricks notebook source
val tableLocation = "`delta`.`s3://tpcds-results-test/lhbench/databases/file_count_benchmark_delta_1k/store_sales`"
// val tableLocation = "file_count_benchmark_delta_1k.store_sales"

val benchmarkId = "filecount_dbr_v1"
val csvReportUploadPath = "s3://tpcds-results-test/lhbench/reports"

// COMMAND ----------

// MAGIC %run ./includes/util

// COMMAND ----------


def getQueries(tableName: String):Seq[(String, String)] = {
    Seq(
      "1. select-limit-1" -> s"SELECT * FROM $tableName LIMIT 1",
      "2. full-count" -> s"SELECT COUNT(*) FROM $tableName",
      "3. filter-by-partition" -> s"SELECT SUM(ss_quantity) FROM $tableName WHERE ss_sold_date_sk = 100",
      "4. filter-by-value" -> s"SELECT SUM(ss_quantity) FROM $tableName WHERE ss_item_sk = 100",
  )
}

// COMMAND ----------

display(spark.sql(s"DESCRIBE DETAIL $tableLocation"))

// COMMAND ----------


val queries = getQueries(tableLocation)

queries.foreach { case (name, query) => 
for( i <- 1 to 4){
  val queryPlanningTimeMs = measureQueryPlanningTimeMs(queryName = name) {       
      runQuery(query, queryName = name, iteration = Some(i), ignoreError = false)
    }   
  }
  log("-" * 80)
}

// COMMAND ----------

generateCSVReport()

// COMMAND ----------


