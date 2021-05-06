package de.flapdoodle.sqlextract.db

interface TableResolver {
    fun byName(name: String): Table?
}