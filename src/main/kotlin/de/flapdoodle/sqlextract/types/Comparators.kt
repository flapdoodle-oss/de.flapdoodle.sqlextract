package de.flapdoodle.sqlextract.types

import com.google.common.collect.Ordering

object Comparators {
    fun <T, C: Comparable<C>> orderingFor(property: (T) -> C): Ordering<T> {
        return Ordering.natural<C>()
            .nullsFirst<C>()
            .onResultOf { t -> if (t!=null) property(t) else null }
    }

    fun <T, C> orderingFor(comparator: Comparator<C>, property: (T) -> C): Ordering<T> {
        return Ordering.from(comparator)
            .onResultOf {  t -> if (t!=null) property(t) else null   }
    }
}