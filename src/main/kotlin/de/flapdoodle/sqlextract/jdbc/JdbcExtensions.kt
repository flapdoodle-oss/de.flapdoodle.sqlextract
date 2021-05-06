package de.flapdoodle.sqlextract.jdbc

import java.sql.DatabaseMetaData
import java.sql.ResultSet

fun DatabaseMetaData.map(resultSetFactory: DatabaseMetaData.() -> ResultSet): ResultSetAdapter {
    val meta = this
    return ResultSetAdapter { resultSetFactory(meta) }
}
