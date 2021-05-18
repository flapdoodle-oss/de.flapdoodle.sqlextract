package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.filetypes.Attributes

data class TableFilterList(
    val filter: List<TableFilter>
) {
    private val filterBySchema = filter.associateBy { it.schema }
    init {
        val groupedBySchema = filter.groupBy { it.schema }
        val sameSchema = groupedBySchema.filter { it.value.size!=1 }
        require(sameSchema.isEmpty()) {"same schema used more than once: $sameSchema"}
    }

    fun matchingTableName(name: Name): Boolean {
        val match = filterBySchema[name.schema]
        
        return match?.matchingTableName(name) ?: true
    }

    companion object {
        fun parse(source: Attributes.Node?): TableFilterList {
            val tableFilter = source?.nodeKeys()?.map {
                TableFilter.parse(it, source.get(it, Attributes.Node::class))
            }

            return TableFilterList(tableFilter ?: emptyList())
        }
    }
}
