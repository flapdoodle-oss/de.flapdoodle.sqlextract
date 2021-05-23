package de.flapdoodle.sqlextract.db

interface TableRepository {
    fun all(): List<Table>
    fun get(name: Name): Table
}