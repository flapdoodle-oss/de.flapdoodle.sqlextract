package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.types.Comparators

data class Reference(
    val sourceTable: Name,
    val sourceColumn: String,

    val destinationTable: Name,
    var destinationColumn: String
) {
    companion object {
        val Comparator = Comparators.orderingFor(Name.Comparator,Reference::sourceTable)
            .then(Comparators.orderingFor(Reference::sourceColumn))
            .then(Comparators.orderingFor(Name.Comparator, Reference::destinationTable))
            .then(Comparators.orderingFor(Reference::destinationColumn))
    }
}
