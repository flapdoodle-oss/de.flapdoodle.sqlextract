package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.filetypes.Attributes

data class Constraint(
    val table: Name,
    val where: List<String>,
    val limit: Long?,
    val orderBy: List<String>
) {

    companion object {
        fun parse(source: Attributes.Node): Constraint {
            val table = source.values("table", String::class).singleOrNull()
            val where = source.findValues("where", String::class)
            val limit = source.findValues("limit", Long::class)?.singleOrNull()
            val orderBy = source.findValues("orderBy", String::class)

            require(table != null) { "table is not set" }

            return Constraint(
                table = Name.parse(table),
                where = where ?: emptyList(),
                limit = limit,
                orderBy = orderBy ?: emptyList()
            )
        }
    }
}
