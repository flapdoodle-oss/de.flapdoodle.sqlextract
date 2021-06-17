package de.flapdoodle.sqlextract.jdbc

import de.flapdoodle.sqlextract.db.Name
import java.sql.*

fun DatabaseMetaData.query(resultSetFactory: DatabaseMetaData.() -> ResultSet): ResultSetAdapter {
    val meta = this
    return ResultSetAdapter { resultSetFactory(meta) }
}

fun Connection.query(resultSetFactory: Connection.() -> ResultSet): ResultSetAdapter {
    val con = this
    return ResultSetAdapter { resultSetFactory(con) }
}

fun DatabaseMetaData.table(name: String, schema: String?): JdbcTable {
    val ret = query { getTables(null, schema, name, arrayOf("TABLE")) }
        .map(JdbcTable.rowMapper())
    require(ret.size == 1) { "more or less then one entry for $name (schema: $schema): $ret" }
    return ret.single()
}

fun DatabaseMetaData.tables(): List<JdbcTable> {
    return query { getTables(null, null, "%", arrayOf("TABLE")) }
        .map(JdbcTable.rowMapper())
}

fun ResultSet.andCloseAfterUse(closeable: AutoCloseable): ResultSetCloseDelegate {
    return ResultSetCloseDelegate(this,closeable)
}

class ResultSetCloseDelegate(
    val wrapped: ResultSet,
    val closeable: AutoCloseable
) : ResultSet by wrapped {
    override fun close() {
        wrapped.close()
        closeable.close()
    }
}

data class JdbcTable(
    val name: Name,
    val type: String,
    val remarks: String?
) {
    companion object {
        fun rowMapper(): ResultSetRow.() -> JdbcTable {
            return {
                val catalog = column(setOf("TABLE_CAT", "TABLE_CATALOG"), String::class)
                val schema = expectColumn(setOf("TABLE_SCHEMA", "TABLE_SCHEM"), String::class)
                val name = expectColumn("TABLE_NAME", String::class)
                val type = expectColumn("TABLE_TYPE", String::class)
                val remarks = column("REMARKS", String::class)
                JdbcTable(
                    name = Name(
                        name = name,
                        schema = schema
                    ),
                    type = type,
                    remarks = remarks
                )
            }
        }
    }
}