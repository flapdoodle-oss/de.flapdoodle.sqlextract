package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.filetypes.Attributes
import java.nio.file.Path

data class DataSet(
        val name: String,
        val table: String,
        val where: String,
        val include: List<String>
) {

    companion object {
        fun parse(name: String, source: Attributes.Node): DataSet {
            val table = source.values("table", String::class).singleOrNull()
            val where = source.values("where", String::class).singleOrNull()
            val include = source.findValues("include", String::class)

            require(table != null) { "table is not set" }
            require(where != null) { "where is not set" }

            return DataSet(name, table, where, include ?: emptyList())
        }
    }
}
