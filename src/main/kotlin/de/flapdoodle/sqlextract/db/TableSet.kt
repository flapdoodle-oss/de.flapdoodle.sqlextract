package de.flapdoodle.sqlextract.db

interface TableSet {
    fun all(): List<Table>
}