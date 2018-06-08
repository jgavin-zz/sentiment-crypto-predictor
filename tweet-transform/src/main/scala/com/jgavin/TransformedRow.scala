package com.jgavin

import com.spotify.scio.bigquery.TableRow
import org.joda.time.Instant

case class TransformedRow(roundedHour: Instant, averageScore: Double, minScore: Double, maxScore: Double, count: Int, nextHoursPercentChange: Double){
  def toTableRow(): TableRow = {
    TableRow(
      "rounded_hour" -> roundedHour.toString,
      "average_score" -> averageScore,
      "min_score" -> minScore,
      "max_score" -> maxScore,
      "count" -> count,
      "next_hours_percent_change" -> nextHoursPercentChange
    )
  }
}