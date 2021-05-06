package de.flapdoodle.sqlextract.jdbc

import java.sql.DatabaseMetaData
import java.sql.ResultSet

interface ResultSetMapper {
    fun <T> map(rowMapper: ResultSetRow.() -> T): List<T>
}

fun DatabaseMetaData.map(resultSetFactory: (DatabaseMetaData) -> ResultSet): ResultSetMapper {
    val meta = this

    return object : ResultSetMapper {
        override fun <T> map(rowMapper: (ResultSetRow) -> T): List<T> {
            val resultSet = resultSetFactory(meta)
            resultSet.use {
                var list = emptyList<T>()
                while (resultSet.next()) {
                    list = list + rowMapper(ResultSetRow(resultSet))
                }
                return list
            }
        }
    }
}
