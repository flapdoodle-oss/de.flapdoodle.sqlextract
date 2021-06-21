package de.flapdoodle.sqlextract.jdbc

import de.flapdoodle.sqlextract.db.Name
import java.sql.DatabaseMetaData

object TableNames {
    fun table(metaData: DatabaseMetaData, name: String, schema: String?): Name {
        val ret = metaData.query { getTables(null, schema, name, arrayOf("TABLE")) }
            .map(rowMapper())
        require(ret.size == 1) { "more or less then one entry for $name (schema: $schema): $ret" }
        return ret.single()
    }

    fun tables(metaData: DatabaseMetaData): List<Name> {
        return metaData.query { getTables(null, null, "%", arrayOf("TABLE")) }
            .map(rowMapper())
    }

    private fun rowMapper(): Row.() -> Name {
        return {
//                val catalog = column(setOf("TABLE_CAT", "TABLE_CATALOG"), String::class)
            val schema = expectColumn(setOf("TABLE_SCHEMA", "TABLE_SCHEM"), String::class)
            val name = expectColumn("TABLE_NAME", String::class)
            val type = expectColumn("TABLE_TYPE", String::class)
            val remarks = column("REMARKS", String::class)

            Name(
                name = name,
                schema = schema
            )
        }
    }

}