package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.filetypes.Attributes
import java.nio.file.Path

data class DataSet(
        val name: String,
        val table: Name,
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
            require(table.indexOf('.') != -1) { "table schema unknown: $table" }

            val idx = table.indexOf('.')
            val tableName=table.substring(idx+1)
            val schema = table.substring(0,idx)

            return DataSet(name, Name(tableName,null,schema), where, include ?: emptyList())
        }
    }
}
