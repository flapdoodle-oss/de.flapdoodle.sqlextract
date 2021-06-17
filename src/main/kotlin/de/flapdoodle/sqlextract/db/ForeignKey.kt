package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.types.Comparators

data class ForeignKey(
    override val sourceTable: Name,
    override val sourceColumn: String,

    override val destinationTable: Name,
    override var destinationColumn: String
): ColumnConnection {
    companion object {
        val Comparator = Comparators.orderingFor(Name.Comparator,ForeignKey::sourceTable)
            .then(Comparators.orderingFor(ForeignKey::sourceColumn))
            .then(Comparators.orderingFor(Name.Comparator, ForeignKey::destinationTable))
            .then(Comparators.orderingFor(ForeignKey::destinationColumn))
    }
}
