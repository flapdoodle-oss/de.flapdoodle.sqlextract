package de.flapdoodle.sqlextract.jdbc

import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet

fun DatabaseMetaData.query(resultSetFactory: DatabaseMetaData.() -> ResultSet): ResultSetAdapter {
    val meta = this
    return ResultSetAdapter { resultSetFactory(meta) }
}

fun Connection.query(resultSetFactory: Connection.() -> ResultSet): ResultSetAdapter {
    val con = this
    return ResultSetAdapter { resultSetFactory(con) }
}