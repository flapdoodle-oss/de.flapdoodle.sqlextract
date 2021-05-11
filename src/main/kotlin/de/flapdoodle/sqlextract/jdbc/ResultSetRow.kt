package de.flapdoodle.sqlextract.jdbc

import java.sql.JDBCType
import java.sql.ResultSet
import kotlin.reflect.KClass

class ResultSetRow(private val wrapped: ResultSet) {
    val meta = wrapped.metaData
    val columnTypeMap = (1..meta.columnCount).map {
        val name = meta.getColumnName(it)
        val type = meta.getColumnType(it)

        name to JDBCType.valueOf(type)
    }.toMap()


    fun column(columnName: String): Any? {
        return wrapped.getObject(columnName)
    }
    
    fun <T: Any> column(columnName: String, type: KClass<T>): T? {
        val columnType = columnTypeMap[columnName]
        require(columnType != null) { "column $columnName not found" }
        return ColumnTypeConverters.converter(columnType, type)(wrapped, columnName)
    }

    fun <T: Any> expectColumn(columnName: String, type: KClass<T>): T {
        val value = column(columnName, type)

        require(value!=null) {"column $columnName is null"}

        return value
    }
}