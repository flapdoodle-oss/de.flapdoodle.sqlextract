package de.flapdoodle.sqlextract.jdbc

import java.sql.ResultSet

class ResultSetAdapter(private val factory: () -> ResultSet) {

    fun <T> map(rowMapper: ResultSetRow.() -> T): List<T> {
        val resultSet = factory()
        resultSet.use {
            var list = emptyList<T>()
            while (resultSet.next()) {
                list = list + rowMapper(ResultSetRow(resultSet))
            }
            return list
        }
    }

    fun <T> findOne(rowMapper: ResultSetRow.() -> T): T? {
        val list = map(rowMapper)
        require(list.size<=1) {"more than one result found: $list"}
        return list.singleOrNull()
    }

    fun <T> get(rowMapper: ResultSetRow.() -> T): T {
        val mapped = map(rowMapper)
        require(mapped.size==1) {"more than one element: $mapped"}
        return mapped.single()
    }
}