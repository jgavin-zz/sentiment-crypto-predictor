package com.jgavin

import com.spotify.scio.bigquery._
import org.joda.time.Instant

case class BtcPricingRow(timestamp: Instant, roundedHour: Instant, last: Double)
object BtcPricingRow {
  def apply(row: TableRow): BtcPricingRow = {
    val timestamp = row.getTimestamp("timestamp")
    BtcPricingRow(
      timestamp,
      timestamp.toDateTime.hourOfDay().roundFloorCopy().toInstant,
      row.getDouble("last")
    )
  }
}