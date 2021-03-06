package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.ForeignKey
import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.filetypes.Attributes

// TODO foreign key points to primary key - must enforce this
data class ForeignKeys(
    val name: String,
    val schema: String,
    val list: List<ForeignKey>
) {

    fun foreignKeys(tableName: Name): List<ForeignKey> {
        return list.filter {
            it.sourceTable == tableName
        }
    }

    companion object {
        fun parse(source: Attributes.Node?): List<ForeignKeys> {
            val foreignKeys = source?.nodeKeys()?.map {
                parse(it, source.get(it, Attributes.Node::class))
            }

            return foreignKeys ?: emptyList()
        }

        fun parse(name: String, source: Attributes.Node): ForeignKeys {
            val schema = source.values("schema", String::class).singleOrNull()
            require(schema != null) { "schema not defined" }

            val foreignKeyList = parse(schema, source.findValues("keys", List::class))


            return ForeignKeys(
                name = name,
                schema = schema,
                list = foreignKeyList
            )
        }

        fun parse(schema: String, table: List<List<*>>?): List<ForeignKey> {
            val src: List<List<*>> = (table ?: emptyList())
            val mapped = src.map {
                require(it.size == 2) { "wrong format, must contain source and destination" }
                require(it[0] is String) { "can not handle first part of: $it" }
                require(it[1] is String) { "can not handle first part of: $it" }
                val source: String = it[0] as String
                val destination: String = it[1] as String
                foreignKey(schema, source, destination)
            }

            return mapped
        }

        private fun foreignKey(schema: String, source: String, destination: String): ForeignKey {
            val s = TableAndColumns.parse(schema, source)
            val d = TableAndColumns.parse(schema, destination)
            return ForeignKey(
                sourceTable = s.first,
                sourceColumn = s.second,
                destinationTable = d.first,
                destinationColumn = d.second
            )
        }
    }
}