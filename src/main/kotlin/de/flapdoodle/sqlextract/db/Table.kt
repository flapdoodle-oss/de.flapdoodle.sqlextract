package de.flapdoodle.sqlextract.db

data class Table(
    val name: String,
    val columns: Set<Column>,
    val primaryKeys: Set<PrimaryKey>,
    val foreignKeys: Set<ForeignKey>
) {
    fun withForeignKeys(keys: List<ForeignKey>): Table {
        return copy(foreignKeys = this.foreignKeys + keys)
    }
}
