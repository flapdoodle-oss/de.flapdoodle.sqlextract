package de.flapdoodle.sqlextract.jdbc

import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.cast

class ResultSetRow(private val wrapped: ResultSet) {
    fun <T: Any> column(columnName: String, type: KClass<T>): T? {
        val value = wrapped.getObject(columnName)
        if (value!=null) {
            return type.cast(value)
        }
        return null
    }
}