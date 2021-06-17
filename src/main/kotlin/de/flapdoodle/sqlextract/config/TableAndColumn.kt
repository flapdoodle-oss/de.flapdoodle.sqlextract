package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name

typealias TableAndColumn = Pair<Name, String>

object TableAndColumns {
    fun parse(schema: String, tableAndColumn: String): TableAndColumn {
        val idx = tableAndColumn.indexOf(':')
        require(idx != -1) { "wrong format: $tableAndColumn != <TABLE:COLUMN>" }
        require(idx > 0) { "missing SCHEMA.TABLE: $tableAndColumn != <TABLE:COLUMN>" }
        require(idx < tableAndColumn.length) { "missing COLUMN: $tableAndColumn != <TABLE:COLUMN>" }
        return Name(tableAndColumn.substring(0, idx), schema) to tableAndColumn.substring(idx + 1)
    }
}