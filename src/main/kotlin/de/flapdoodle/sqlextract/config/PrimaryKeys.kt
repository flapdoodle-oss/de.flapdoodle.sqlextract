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
                TableAndColumns.parse(schema, it)
            }

            return mapped
        }
    }
}