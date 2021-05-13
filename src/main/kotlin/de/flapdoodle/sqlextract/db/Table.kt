package de.flapdoodle.sqlextract.db

data class Table(
    val name: String,
    val columns: Set<Column> = emptySet(),
    val primaryKeys: Set<PrimaryKey> = emptySet(),
    val foreignKeys: Set<ForeignKey> = emptySet()
) {
    init {
        require(foreignKeys.all { it.sourceTable == name }) { "invalid foreignKey: $this" }
    }

    fun withForeignKeys(keys: List<ForeignKey>): Table {
        return copy(foreignKeys = this.foreignKeys + keys)
    }
}
