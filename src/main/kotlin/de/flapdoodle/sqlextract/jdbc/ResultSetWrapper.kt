package de.flapdoodle.sqlextract.jdbc

import java.sql.ResultSet

class ResultSetWrapper(
    private val factory: () -> ResultSet
) {
    fun <T> map(rowMapper: Row.() -> T): List<T> {
        val resultSet = factory()
        resultSet.use {
            var list = emptyList<T>()
            while (resultSet.next()) {
                list = list + rowMapper(Row(resultSet))
            }
            return list
        }
    }

    fun <T> get(rowMapper: Row.() -> T): T {
        val mapped = map(rowMapper)
        require(mapped.size==1) {"more than one element: $mapped"}
        return mapped.single()
    }
}