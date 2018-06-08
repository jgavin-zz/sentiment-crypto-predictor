package com.jgavin

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio._
import com.spotify.scio.bigquery._
import org.joda.time.{Duration, Instant}

import scala.collection.JavaConverters._

/*
  sbt -Dbigquery.project=crypto-pred195103
  runMain com.jgavin.TweetTransform --project=[PROJECT-ID] --runner=DataflowRunner --zone=us-central1-b --output=rtda.transformed_tweets
  runMain com.jgavin.TweetTransform --project=crypto-[PROJECT-ID] --runner=DirectRunner --zone=us-central1-b --output=rtda.transformed_tweets
*/

object TweetTransform {

  def transform(enhancedRows: Iterable[EnhancedRow], btcPricingRowsOption: Option[Iterable[BtcPricingRow]]): Option[TransformedRow] = {
    btcPricingRowsOption.map(btcPricingRows => {
      val count = enhancedRows.size
      val scores = enhancedRows.map(row => row.polarity * row.magnitude)
      val averageScore = scores.sum / count
      val minScore = scores.min
      val maxScore = scores.max

      val entryPrice = btcPricingRows.minBy(_.timestamp.getMillis).last
      val exitPrice = btcPricingRows.maxBy(_.timestamp.getMillis).last
      val percentChange = ((exitPrice - entryPrice) / entryPrice) * 100

      TransformedRow(
        enhancedRows.head.roundedHour,
        averageScore,
        minScore,
        maxScore,
        count,
        percentChange
      )
    })

  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val outputTable = args("output")
    val windowSize = Duration.standardMinutes(60)

    val schema = new TableSchema().setFields(List(
      new TableFieldSchema().setName("rounded_hour").setType("TIMESTAMP"),
      new TableFieldSchema().setName("average_score").setType("FLOAT"),
      new TableFieldSchema().setName("min_score").setType("FLOAT"),
      new TableFieldSchema().setName("max_score").setType("FLOAT"),
      new TableFieldSchema().setName("count").setType("INTEGER"),
      new TableFieldSchema().setName("next_hours_percent_change").setType("FLOAT")
    ).asJava)

    val enhancedTweetQuery = "SELECT created_at, magnitude, polarity FROM `rtda.enhanced_tweets`;"
    val btcPricingQuery = "SELECT timestamp, last FROM `rtda.btc_pricing`;"

    val enhancedTweetRows = sc.bigQuerySelect(enhancedTweetQuery).map(row => EnhancedRow(row)).groupBy(_.roundedHour)
    val btcPricingRows = sc.bigQuerySelect(btcPricingQuery).map(row => BtcPricingRow(row)).groupBy(_.roundedHour)
    enhancedTweetRows
      .leftOuterJoin(btcPricingRows)
      .flatMap(joinedRows => transform(joinedRows._2._1, joinedRows._2._2))
      .map(_.toTableRow())
      .saveAsBigQuery(args("output"), schema, WRITE_TRUNCATE, CREATE_IF_NEEDED)
    sc.close()
  }
}
