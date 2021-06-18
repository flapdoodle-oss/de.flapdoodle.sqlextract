package de.flapdoodle.sqlextract.db

import com.google.common.collect.Ordering
import de.flapdoodle.sqlextract.types.Comparators

data class Name(
    val name: String,
    val schema: String,
) {
    fun asSQL(): String {
        return "$schema.$name"
    }

    fun asId(): String {
        return schema+"_"+name
    }

    companion object {
        val Comparator = Comparators.orderingFor(Name::schema)
            .then(Comparators.orderingFor(Name::name))

        fun parse(name: String): Name {
            val idx = name.indexOf('.')
            require(idx != -1) { "invalid format: '$name', expected: SCHEMA.TABLE" }
            return Name(name = name.substring(idx + 1), schema = name.substring(0, idx))
        }
    }
}
