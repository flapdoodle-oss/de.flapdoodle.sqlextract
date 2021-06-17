package de.flapdoodle.sqlextract.db

interface ColumnConnection {
    val sourceTable: Name
    val sourceColumn: String

    val destinationTable: Name
    var destinationColumn: String
}