package de.flapdoodle.sqlextract.db

import java.sql.Connection

fun interface TableNamesFactory {
    fun tableNames(connection: Connection): List<Name>
}