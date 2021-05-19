package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.ForeignKey
import de.flapdoodle.sqlextract.filetypes.Attributes

data class ForeignKeys(
    val name: String,
    val schema: String,
    val list: List<ForeignKey>
) {

    fun foreignKeys(tableName: String): List<ForeignKey> {
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
            val foreignKeyList = parse(source.findValues("keys", List::class))
            val schema = source.values("schema", String::class).singleOrNull()

            require(schema != null) { "schema not defined" }

            return ForeignKeys(
                name = name,
                schema = schema,
                list = foreignKeyList
            )
        }

        fun parse(table: List<List<*>>?): List<ForeignKey> {
            val src: List<List<*>> = (table ?: emptyList())
            val mapped = src.map {
                require(it.size == 2) { "wrong format, must contain source and destination" }
                require(it[0] is String) { "can not handle first part of: $it" }
                require(it[1] is String) { "can not handle first part of: $it" }
                val source: String = it[0] as String
                val destination: String = it[1] as String
                foreignKey(source, destination)
            }

            return mapped
        }

        private fun foreignKey(source: String, destination: String): ForeignKey {
            val s = tableAndColumn(source)
            val d = tableAndColumn(destination)
            return ForeignKey(
                sourceTable = s.first,
                sourceColumn = s.second,
                destinationTable = d.first,
                destinationColumn = d.second
            )
        }

        private fun tableAndColumn(value: String): Pair<String, String> {
            val idx = value.indexOf('.')
            require(idx != -1) { "wrong format: $value != <TABLE.COLUMN>" }
            return value.substring(0, idx) to value.substring(idx + 1)
        }
    }
}