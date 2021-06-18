package de.flapdoodle.sqlextract.data

import de.flapdoodle.sqlextract.db.Name

class TableRows(val table: Name) {
    private var rowMap = emptyMap<RowKey, TableRow>()

    fun add(row: TableRow) {
        require(row.table.name == table) { "wrong table: $row != $table" }
        if (!rowMap.containsKey(row.key)) {
            rowMap = rowMap + (row.key to row)
        }
    }

    fun contains(row: TableRow): Boolean {
        return rowMap.containsKey(row.key)
    }

    fun rows() = rowMap.values
}