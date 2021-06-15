package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.filetypes.Attributes

data class DataSet(
        val name: String,
        val table: Name,
        val where: List<String>,
        val limit: Long?,
        val orderBy: List<String>,
        val backtrack: List<Backtrack>
) {
    init {
        val multiplyDefinedBacktracks = backtrack.map { it.source to it.destination }
            .groupBy { it }
            .filterValues { it.size!=1 }
        require(multiplyDefinedBacktracks.isEmpty()) {"backtrack defined more than once: ${multiplyDefinedBacktracks.keys} "}
    }

    companion object {
        fun parse(name: String, source: Attributes.Node): DataSet {
            val table = source.values("table", String::class).singleOrNull()
            val where = source.values("where", String::class)
            val limit = source.findValues("limit", Long::class)?.singleOrNull()
            val orderBy = source.findValues("orderBy", String::class)

            require(table != null) { "table is not set" }
            require(where.isNotEmpty()) { "where is not set: $where" }

            val backtrack = source.findValues("backtrack", Attributes.Node::class)?.map {
                Backtrack.parse(it)
            }

            return DataSet(
                name = name,
                table = Name.parse(table),
                where = where,
                limit = limit,
                orderBy = orderBy ?: emptyList(),
                backtrack = backtrack ?: emptyList()
            )
        }
    }
}
