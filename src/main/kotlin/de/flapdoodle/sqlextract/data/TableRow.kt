package de.flapdoodle.sqlextract.data

import de.flapdoodle.sqlextract.db.Table

data class TableRow(val table: Table, val key: RowKey, val values: Map<String, Any?>) {

    companion object {
        fun of(table: Table, values: Map<String, Any?>): TableRow {
            return TableRow(table, rowKey(table, values), values)
        }

        private fun rowKey(table: Table, values: Map<String, Any?>): RowKey {
            require(table.primaryKeys.isNotEmpty()) {"table ${table.name} does not have any primary keys"}
            return RowKey(table, table.primaryKeys.map {
                it.columnName to knownType(values[it.columnName])
            }.toMap())
        }

        private fun <T> knownType(value: T?): T? {
            return value
        }
    }
}