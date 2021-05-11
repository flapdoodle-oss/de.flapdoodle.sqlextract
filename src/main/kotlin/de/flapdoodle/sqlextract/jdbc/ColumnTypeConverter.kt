package de.flapdoodle.sqlextract.jdbc

import java.sql.JDBCType
import java.sql.ResultSet
import kotlin.reflect.KClass

data class ColumnTypeConverter<T: Any>(
    val columnType: JDBCType,
    val type: KClass<T>,
    val converter: (resultSet: ResultSet, columnName: String) -> T?
)