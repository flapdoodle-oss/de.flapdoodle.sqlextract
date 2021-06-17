package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.types.Comparators

data class Reference(
    override val sourceTable: Name,
    override val sourceColumn: String,

    override val destinationTable: Name,
    override var destinationColumn: String
): ColumnConnection {
    companion object {
        val Comparator = Comparators.orderingFor(Name.Comparator,Reference::sourceTable)
            .then(Comparators.orderingFor(Reference::sourceColumn))
            .then(Comparators.orderingFor(Name.Comparator, Reference::destinationTable))
            .then(Comparators.orderingFor(Reference::destinationColumn))
    }
}
