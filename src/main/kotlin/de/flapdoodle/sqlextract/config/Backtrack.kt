package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.filetypes.Attributes

data class Backtrack(
        val source: Name,
        val destination: Name,
        val where: List<String>,
        val limit: Long?,
        val orderBy: List<String>
) {

    companion object {
        fun parse(source: Attributes.Node): Backtrack {
            val sourceTable = source.values("source", String::class).singleOrNull()
            val destinationTable = source.values("destination", String::class).singleOrNull()

            val where = source.findValues("where", String::class)
            val limit = source.findValues("limit", Long::class)?.singleOrNull()
            val orderBy = source.findValues("orderBy", String::class)

            require(sourceTable != null) { "source is not set" }
            require(destinationTable != null) { "destination is not set" }

            return Backtrack(
                source = Name.parse(sourceTable),
                destination = Name.parse(destinationTable),
                where = where ?: emptyList(),
                limit = limit,
                orderBy = orderBy ?: emptyList()
            )
        }
    }
}
