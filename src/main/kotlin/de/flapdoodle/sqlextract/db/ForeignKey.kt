package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.types.Comparators

data class ForeignKey(
    val sourceTable: Name,
    val sourceColumn: String,

    val destinationTable: Name,
    var destinationColumn: String
) {
    companion object {
        val Comparator = Comparators.orderingFor(Name.Comparator,ForeignKey::sourceTable)
            .then(Comparators.orderingFor(ForeignKey::sourceColumn))
            .then(Comparators.orderingFor(Name.Comparator, ForeignKey::destinationTable))
            .then(Comparators.orderingFor(ForeignKey::destinationColumn))
    }
}
