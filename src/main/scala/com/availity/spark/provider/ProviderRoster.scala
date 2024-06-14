// ProviderRoster.scala

package com.availity.spark.provider

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ProviderRoster {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Provider Visits")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    // Load the providers.csv
    val providersDF = spark.read
      .schema("provider_id INT, provider_specialty STRING, first_name STRING, middle_name STRING, last_name STRING")
      .option("delimiter", "|")
      .csv("data/providers.csv")

    // Load the visits.csv
    val visitsDF = spark.read
      .option("header", "false")
      .schema("visit_id INT, provider_id INT, date_of_service STRING")
      .csv("data/visits.csv")

    // Create views providers, visits to use them in SQL queries
    providersDF.createOrReplaceTempView("providers")
    visitsDF.createOrReplaceTempView("visits")

    // Task 1: Calculate the total number of visits per provider
    val totalVisitsPerProviderSQL = spark.sql(
      """
        |SELECT
        |  p.provider_id,
        |  CONCAT_WS(' ', p.first_name, p.middle_name, p.last_name) AS name,
        |  p.provider_specialty,
        |  COUNT(v.visit_id) AS total_visits
        |FROM providers p
        |LEFT JOIN visits v
        |  ON p.provider_id = v.provider_id
        |GROUP BY p.provider_id, p.first_name, p.middle_name, p.last_name, p.provider_specialty
      """.stripMargin)

    totalVisitsPerProviderSQL
      .repartition($"provider_specialty")
      .write
      .partitionBy("provider_specialty")
      .mode("overwrite")
      .json("output/total_visits_per_provider")

    // Task 2: Calculate the total number of visits per provider per month
    val visitsPerProviderPerMonthSQL = spark.sql(
      """
        |SELECT
        |  v.provider_id,
        |  DATE_FORMAT(TO_DATE(v.date_of_service, 'yyyy-MM-dd'), 'yyyy-MM') AS month,
        |  COUNT(v.visit_id) AS total_visits
        |FROM visits v
        |GROUP BY v.provider_id, month
      """.stripMargin)

    visitsPerProviderPerMonthSQL
      .repartition($"provider_id")
      .write
      .mode("overwrite")
      .json("output/total_visits_per_provider_per_month")

    spark.stop()
  }
}