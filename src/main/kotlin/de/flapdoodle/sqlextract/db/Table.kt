package de.flapdoodle.sqlextract.db

data class Table(
        val name: Name,
        val columns: Set<Column> = emptySet(),
        val primaryKeys: Set<PrimaryKey> = emptySet(),
        val foreignKeys: Set<ForeignKey> = emptySet()
) {
    init {
        val columnNames = columns.map { it.name }
        require(foreignKeys.all { it.sourceTable == name }) { "invalid foreignKey: $this" }
        require(foreignKeys.all {  columnNames.contains(it.sourceColumn) }) {
            val unknown = foreignKeys.filter { !columnNames.contains(it.sourceColumn) }
            "foreignKeys contains invalid column names: $unknown - $this "
        }
    }

    fun withForeignKeys(keys: List<ForeignKey>): Table {
        return copy(foreignKeys = this.foreignKeys + keys)
    }

    fun destinationTables(): Set<Name> {
        return foreignKeys.map {
            it.destinationTable
        }.toSet()
    }
}
