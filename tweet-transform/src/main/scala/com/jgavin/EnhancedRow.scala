package com.jgavin

import com.spotify.scio.bigquery._
import org.joda.time.Instant

case class EnhancedRow(createdAt: Instant, roundedHour: Instant, magnitude: Double, polarity: Double)
object EnhancedRow {
  def apply(row: TableRow): EnhancedRow = {
    val createdAt = row.getTimestamp("created_at")
    EnhancedRow(
      createdAt,
      createdAt.toDateTime.hourOfDay().roundCeilingCopy().toInstant,
      row.getDouble("magnitude"),
      row.getDouble("polarity")
    )
  }
}