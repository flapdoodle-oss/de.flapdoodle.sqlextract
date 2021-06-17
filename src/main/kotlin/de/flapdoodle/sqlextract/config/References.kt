package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.db.Reference
import de.flapdoodle.sqlextract.filetypes.Attributes

data class References(
    val name: String,
    val schema: String,
    val list: List<Reference>
) {

    fun references(tableName: Name): List<Reference> {
        return list.filter {
            it.sourceTable == tableName
        }
    }

    companion object {
        fun parse(source: Attributes.Node?): List<References> {
            val foreignKeys = source?.nodeKeys()?.map {
                parse(it, source.get(it, Attributes.Node::class))
            }

            return foreignKeys ?: emptyList()
        }

        fun parse(name: String, source: Attributes.Node): References {
            val schema = source.values("schema", String::class).singleOrNull()
            require(schema != null) { "schema not defined" }

            val referenceList = parse(schema, source.findValues("keys", List::class))


            return References(
                name = name,
                schema = schema,
                list = referenceList
            )
        }

        fun parse(schema: String, table: List<List<*>>?): List<Reference> {
            val src: List<List<*>> = (table ?: emptyList())
            val mapped = src.map {
                require(it.size == 2) { "wrong format, must contain source and destination" }
                require(it[0] is String) { "can not handle first part of: $it" }
                require(it[1] is String) { "can not handle first part of: $it" }
                val source: String = it[0] as String
                val destination: String = it[1] as String
                reference(schema, source, destination)
            }

            return mapped
        }

        private fun reference(schema: String, source: String, destination: String): Reference {
            val s = tableAndColumn(schema, source)
            val d = tableAndColumn(schema, destination)
            return Reference(
                sourceTable = s.first,
                sourceColumn = s.second,
                destinationTable = d.first,
                destinationColumn = d.second
            )
        }

        private fun tableAndColumn(schema: String, value: String): Pair<Name, String> {
            val idx = value.indexOf(':')
            require(idx != -1) { "wrong format: $value != <SCHEMA.TABLE:COLUMN>" }
            return Name(value.substring(0, idx), schema) to value.substring(idx + 1)
        }
    }
}