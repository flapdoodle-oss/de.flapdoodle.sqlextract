package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.db.PrimaryKey
import de.flapdoodle.sqlextract.filetypes.Attributes

data class PrimaryKeys(
    val name: String,
    val schema: String,
    val list: List<Pair<Name, String>>
) {

    fun primaryKeys(tableName: Name): List<PrimaryKey> {
        return list.filter {
            it.first == tableName
        }.map { PrimaryKey(it.second,"${it.second} added by config") }
    }

    companion object {
        fun parse(source: Attributes.Node?): List<PrimaryKeys> {
            val foreignKeys = source?.nodeKeys()?.map {
                parse(it, source.get(it, Attributes.Node::class))
            }

            return foreignKeys ?: emptyList()
        }

        fun parse(name: String, source: Attributes.Node): PrimaryKeys {
            val schema = source.values("schema", String::class).singleOrNull()
            require(schema != null) { "schema not defined" }

            val foreignKeyList = parse(schema, source.findValues("keys", String::class))


            return PrimaryKeys(
                name = name,
                schema = schema,
                list = foreignKeyList
            )
        }

        fun parse(schema: String, src: List<String>?): List<Pair<Name, String>> {
            val mapped = (src?: emptyList()).map {
                tableAndColumn(schema, it)
            }

            return mapped
        }

        private fun tableAndColumn(schema: String, value: String): Pair<Name, String> {
            val idx = value.indexOf(':')
            require(idx != -1) { "wrong format: $value != <SCHEMA.TABLE:COLUMN>" }
            return Name(value.substring(0, idx), schema) to value.substring(idx + 1)
        }
    }
}