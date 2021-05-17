package de.flapdoodle.sqlextract.db

fun interface TableResolver {
    fun byName(name: Name): Table
}