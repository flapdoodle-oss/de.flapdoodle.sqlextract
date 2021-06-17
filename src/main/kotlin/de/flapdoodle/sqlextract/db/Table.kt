package de.flapdoodle.sqlextract.db

data class Table(
        val name: Name,
        val columns: Set<Column> = emptySet(),
        val primaryKeys: Set<PrimaryKey> = emptySet(),
        val foreignKeys: Set<ForeignKey> = emptySet(),
        val references: Set<Reference> = emptySet(),
) {
    init {
        val columnNames = columns.map { it.name }

        require(primaryKeys.all { columnNames.contains(it.columnName) }) {
            "primary keys contains unknown columns: $columnNames - $primaryKeys"
        }

        require(foreignKeys.all { it.sourceTable == name }) { "invalid foreignKey: $this" }

        require(foreignKeys.all {  columnNames.contains(it.sourceColumn) }) {
            val unknown = foreignKeys.filter { !columnNames.contains(it.sourceColumn) }
            "foreignKeys contains invalid column names: $unknown - $this "
        }

        require(references.all { it.sourceTable == name }) { "invalid references: $this" }

        require(references.all {  columnNames.contains(it.sourceColumn) }) {
            val unknown = references.filter { !columnNames.contains(it.sourceColumn) }
            "references contains invalid column names: $unknown - $this "
        }
    }

    fun withForeignKeys(keys: List<ForeignKey>): Table {
        return copy(foreignKeys = this.foreignKeys + keys)
    }

    fun withPrimaryKeys(keys: List<PrimaryKey>): Table {
        return copy(primaryKeys = this.primaryKeys + keys)
    }

    fun withReferences(keys: List<Reference>): Table {
        return copy(references = this.references + keys)
    }

    fun destinationTables(): Set<Name> {
        return foreignKeys.map {
            it.destinationTable
        }.toSet()
    }
}
