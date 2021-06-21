package de.flapdoodle.sqlextract.jdbc

import java.sql.JDBCType
import java.sql.ResultSet
import kotlin.reflect.KClass

class Row(private val wrapped: ResultSet) {
    private val meta = wrapped.metaData
    private val columnTypeMap = (1..meta.columnCount).associate {
        val name = meta.getColumnName(it)
        val type = meta.getColumnType(it)

        name to JDBCType.valueOf(type)
    }


    fun column(columnName: String): Any? {
        return wrapped.getObject(columnName)
    }

    fun <T: Any> column(columnName: Set<String>, type: KClass<T>): T? {
        val matchingColumnName = columnName.first { columnTypeMap.containsKey(it) }
        val columnType = columnTypeMap[matchingColumnName]
        require(columnType != null) { "column $matchingColumnName not found in $columnTypeMap" }
        return ColumnTypeConverters.converter(columnType, type)(wrapped, matchingColumnName)
    }

    fun <T: Any> column(columnName: String, type: KClass<T>): T? {
        val columnType = columnTypeMap[columnName]
        require(columnType != null) { "column $columnName not found in $columnTypeMap" }
        return ColumnTypeConverters.converter(columnType, type)(wrapped, columnName)
    }

    fun <T: Any> expectColumn(columnName: String, type: KClass<T>): T {
        val value = column(columnName, type)
        require(value!=null) {"column $columnName is null"}
        return value
    }

    fun <T: Any> expectColumn(columnName: Set<String>, type: KClass<T>): T {
        val value = column(columnName, type)
        require(value!=null) {"column $columnName is null"}
        return value
    }
}