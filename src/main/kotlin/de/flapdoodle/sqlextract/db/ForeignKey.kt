package de.flapdoodle.sqlextract.db

data class ForeignKey(
    val sourceTable: Name,
    val sourceColumn: String,

    val destinationTable: Name,
    var destinationColumn: String
)
