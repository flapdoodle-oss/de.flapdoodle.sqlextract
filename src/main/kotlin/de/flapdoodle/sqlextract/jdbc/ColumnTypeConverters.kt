package de.flapdoodle.sqlextract.jdbc

import java.math.BigDecimal
import java.sql.JDBCType
import java.sql.ResultSet
import java.util.*
import kotlin.reflect.KClass

object ColumnTypeConverters {
    private val map = MapBuilder()
        .add(JDBCType.INTEGER, Int::class, ResultSet::getInt)
        .add(JDBCType.NUMERIC, Int::class, ResultSet::getInt)
        .add(JDBCType.INTEGER, BigDecimal::class, ResultSet::getBigDecimal)
        .add(JDBCType.VARCHAR, String::class, ResultSet::getString)
        .build()

    fun <T: Any> converter(jdbcType: JDBCType, type: KClass<T>): (ResultSet, String) ->T? {
        if (type==Object::class) {
            return { resultSet, column -> resultSet.getObject(column) as T? }
        }
        val converter = map[jdbcType to type]
        require(converter!=null) {"could not find converter for $jdbcType -> $type"}
        return converter.converter as (ResultSet, String) -> T?
    }

    private class MapBuilder {
        private var map= emptyMap<Pair<JDBCType, KClass<*>>, ColumnTypeConverter<*>>()

        fun <T: Any> add(jdbcType: JDBCType, type: KClass<T>, converter: (ResultSet, String) -> T?): MapBuilder {
            map = map + ((jdbcType to type) to ColumnTypeConverter(jdbcType,type,converter))
            return this;
        }

        fun build(): Map<Pair<JDBCType, KClass<*>>, ColumnTypeConverter<*>> {
            return map
        }
    }
}