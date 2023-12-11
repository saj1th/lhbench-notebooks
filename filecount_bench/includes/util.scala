// Databricks notebook source
import java.time.LocalTime

import scala.collection.mutable
import java.net.URI
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

import scala.language.postfixOps
import scala.sys.process._
import scala.util.control.NonFatal
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.SparkContext
import org.apache.spark.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col}
import scala.util.control.NonFatal
import scala.collection.mutable

val queryResults = new mutable.ArrayBuffer[QueryResult]
val extraMetrics = new mutable.HashMap[String, Double]

// Holds query results
case class QueryResult(
    name: String,
    iteration: Option[Int],
    durationMs: Option[Long],
    errorMsg: Option[String]
)

// Custom SparkListener to capture planning time
class DataProcessingnStartListener(expectedJobName: String)
    extends SparkListener {
  @volatile var processingStartTimeMs = -1L
  @volatile var processingStartTime: Option[LocalTime] = None

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobId = jobStart.jobId
    val jobDesc = jobStart.properties.getProperty("spark.job.description", "")
    log(
      s"DataProcessingnStartListener: Job $jobId [$jobDesc] started, props: " + jobStart.properties
    )
    if (jobDesc.startsWith(expectedJobName) && processingStartTimeMs < 0) {
      processingStartTimeMs = System.currentTimeMillis()
      processingStartTime = Some(LocalTime.now())
    }
  }
}


def log(str: => String): Unit = {
  println(s"${java.time.LocalDateTime.now} $str")
}

def runQuery(
    sqlCmd: String,
    queryName: String = "",
    iteration: Option[Int] = None,
    printRows: Boolean = false,
    ignoreError: Boolean = true
): DataFrame = synchronized {
  val iterationStr = iteration.map(i => s" - iteration $i").getOrElse("")
  var banner = s"$queryName$iterationStr"
  if (banner.trim.isEmpty) {
    banner =
      sqlCmd.split("\n")(0).trim + (if (sqlCmd.split("\n").size > 1) "..."
                                    else "")
  }
  log("=" * 80)
  log(s"START: $banner")
  // log("SQL: " + sqlCmd.replaceAll("\n\\s*", " "))
  spark.sparkContext.setJobGroup(banner, banner, interruptOnCancel = true)
  try {
    val before = System.nanoTime()
    val df = spark.sql(sqlCmd)
    val r = df.collect()
    val after = System.nanoTime()
    if (printRows) df.show(false)
    val durationMs = (after - before) / (1000 * 1000)
    queryResults += QueryResult(
      queryName,
      iteration,
      Some(durationMs),
      errorMsg = None
    )
    log(s"END took $durationMs ms: $banner")
    log("=" * 80)
    df
  } catch {
    case NonFatal(e) =>
      log(s"ERROR: $banner\n${e.getMessage}")
      queryResults +=
        QueryResult(
          queryName,
          iteration,
          durationMs = None,
          errorMsg = Some(e.getMessage)
        )
      if (!ignoreError) throw e else spark.emptyDataFrame
  }
}



def measureQueryPlanningTimeMs(queryName: String)(f: => Unit): Long = {
  val listener = new DataProcessingnStartListener(queryName)
  spark.sparkContext.addSparkListener(listener)
  val queryStartTimeMs = System.currentTimeMillis()
  val queryStartTime = LocalTime.now()
  try {
    f
  } finally {
    spark.sparkContext.removeSparkListener(listener)
  }
  log(s"QueryPlanningMeasurement: Query started at $queryStartTime [$queryStartTimeMs ms], " + s"data job started at ${listener.processingStartTime} [${listener.processingStartTimeMs} ms]")
  if (listener.processingStartTimeMs > 0) {
    val ms = listener.processingStartTimeMs - queryStartTimeMs
    log(s"Query planning took ${ms} ms")
    ms
  } else -1L
}

def reportQueryResult(
    name: String,
    durationMs: Long,
    iteration: Option[Int] = None
): Unit = {
  queryResults += QueryResult(
    name,
    iteration,
    durationMs = Some(durationMs),
    errorMsg = None
  )
}

def getQueryResults(): Array[QueryResult] = synchronized {
  queryResults.toArray
}

def median(seq: Seq[Double]): Double = {
  seq.sorted.drop(math.floor(seq.size / 2.0).toInt).head
}

def median(seq: Seq[Long]): Long = {
  seq.sorted.drop(math.floor(seq.size / 2.0).toInt).head
}


def generateCSVReport(): Unit = synchronized {
  val csvHeader = "name,iteration,durationMs"
  val csvRows = queryResults.map { r =>
    s"${r.name},${r.iteration.getOrElse(1)},${r.durationMs.getOrElse(-1)}"
  }
  val csvText = (Seq(csvHeader) ++ csvRows).mkString("\n")
  val resultFileName =
    if (benchmarkId.trim.isEmpty) "report.csv" else s"$benchmarkId-report.csv"
  val reportLocalPath = Paths.get(resultFileName).toAbsolutePath()
  Files.write(reportLocalPath, csvText.getBytes(StandardCharsets.UTF_8))
  println(reportLocalPath)
  uploadFile(reportLocalPath.toString, csvReportUploadPath)
}

def uploadFile(localPath: String, targetPath: String): Unit = {
  val targetUri = new URI(targetPath)
  val sanitizedTargetPath = targetUri.normalize().toString
  val scheme = new URI(targetPath).getScheme
  try {
    if (scheme.equals("s3")) s"aws s3 cp $localPath $sanitizedTargetPath/" !
    else if (scheme.equals("gs"))
      s"gsutil cp $localPath $sanitizedTargetPath/" !
    else
      throw new IllegalArgumentException(
        String.format("Unsupported scheme %s.", scheme)
      )

    println(s"FILE UPLOAD: Uploaded $localPath to $sanitizedTargetPath")
  } catch {
    case NonFatal(e) =>
      log(
        s"FILE UPLOAD: Failed to upload $localPath to $sanitizedTargetPath: $e"
      )
  }
}

