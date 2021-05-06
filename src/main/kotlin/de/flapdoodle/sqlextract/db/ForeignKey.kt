package de.flapdoodle.sqlextract.db

data class ForeignKey(
    val tableName: String,
    val columnName: String,

    val foreignTableName: String,
    var foreignColumnName: String
)
