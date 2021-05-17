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

fun DatabaseMetaData.table(name: String): JdbcTable {
    val ret = query { getTables(null, null, name, arrayOf("TABLE")) }
            .map(JdbcTable.rowMapper())
    require(ret.size==1) {"more or less then one entry: $ret"}
    return ret.single()
}

fun DatabaseMetaData.tables(): List<JdbcTable> {
    return query { getTables(null, null, "%", arrayOf("TABLE")) }
            .map(JdbcTable.rowMapper())
}

data class JdbcTable(
        val name: String,
        val catalog: String?,
        val schema: String?,
        val type: String,
        val remarks: String?
) {
    companion object {
        fun rowMapper(): ResultSetRow.() -> JdbcTable {
            return {
                val catalog = column("TABLE_CAT", String::class)
                val schema = column("TABLE_SCHEM", String::class)
                val name = expectColumn("TABLE_NAME", String::class)
                val type = expectColumn("TABLE_TYPE", String::class)
                val remarks = column("REMARKS", String::class)
                JdbcTable(
                        catalog = catalog,
                        schema = schema,
                        name = name,
                        type = type,
                        remarks = remarks
                )
            }
        }
    }
}