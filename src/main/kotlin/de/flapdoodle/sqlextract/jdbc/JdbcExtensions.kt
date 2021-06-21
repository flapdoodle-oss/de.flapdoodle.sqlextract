package de.flapdoodle.sqlextract.jdbc

import java.sql.*

fun DatabaseMetaData.query(resultSetFactory: DatabaseMetaData.() -> ResultSet): ResultSetWrapper {
    val meta = this
    return ResultSetWrapper { resultSetFactory(meta) }
}

fun Connection.query(resultSetFactory: Connection.() -> ResultSet): ResultSetWrapper {
    val con = this
    return ResultSetWrapper { resultSetFactory(con) }
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

