package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.filetypes.Attributes

data class DataSet(
        val name: String,
        val table: Name,
        val where: List<String>,
        val limit: Long?,
        val orderBy: List<String>,
        val constraints: List<Constraint>,
        val backtrack: List<Backtrack>
) {

    companion object {
        fun parse(name: String, source: Attributes.Node): DataSet {
            val table = source.values("table", String::class).singleOrNull()
            val where = source.values("where", String::class)
            val limit = source.findValues("limit", Long::class)?.singleOrNull()
            val orderBy = source.findValues("orderBy", String::class)

            require(table != null) { "table is not set" }
            require(where.isNotEmpty()) { "where is not set: $where" }

            val constraints = source.findValues("constraints", Attributes.Node::class)?.map {
                Constraint.parse(it)
            }

            val backtrack = source.findValues("backtrack", Attributes.Node::class)?.map {
                Backtrack.parse(it)
            }

            return DataSet(
                name = name,
                table = Name.parse(table),
                where = where,
                limit = limit,
                orderBy = orderBy ?: emptyList(),
                constraints = constraints ?: emptyList(),
                backtrack = backtrack ?: emptyList()
            )
        }
    }
}
