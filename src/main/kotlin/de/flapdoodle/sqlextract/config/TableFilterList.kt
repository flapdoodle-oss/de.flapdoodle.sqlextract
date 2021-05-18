package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.filetypes.Attributes

data class TableFilterList(
    val filter: List<TableFilter>,
    val excludedSchemas: Set<String>
) {
    private val filterBySchema = filter.associateBy { it.schema }

    init {
        val groupedBySchema = filter.groupBy { it.schema }
        val sameSchema = groupedBySchema.filter { it.value.size != 1 }
        require(sameSchema.isEmpty()) { "same schema used more than once: $sameSchema" }
        val schemaHasFilter = excludedSchemas.filter { filterBySchema.containsKey(it) }
        require(schemaHasFilter.isEmpty()) { "excluded schema has filter definitions: $schemaHasFilter" }
    }

    fun matchingTableName(name: Name): Boolean {
        return if (excludedSchemas.contains(name.schema)) false
        else
            filterBySchema[name.schema]?.matchingTableName(name) ?: true
    }

    companion object {
        fun parse(source: Attributes.Node?): TableFilterList {
            val excludeSchemas = source?.findValues("excludeSchema", String::class)?.toSet()

            val tableFilter = source?.nodeKeys()?.map {
                TableFilter.parse(it, source.get(it, Attributes.Node::class))
            }

            return TableFilterList(
                filter = tableFilter ?: emptyList(),
                excludedSchemas = excludeSchemas ?: emptySet()
            )
        }
    }
}
