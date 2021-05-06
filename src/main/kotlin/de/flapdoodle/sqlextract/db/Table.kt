package de.flapdoodle.sqlextract.db

data class Table(
    val name: String,
    val columns: List<Column>,
    val primaryKeys: List<PrimaryKey>,
    val foreignKeys: List<ForeignKey>
)
