package de.flapdoodle.sqlextract.db

data class ForeignKey(
    val sourceTable: String,
    val sourceColumn: String,

    val destinationTable: String,
    var destinationColumn: String
)
