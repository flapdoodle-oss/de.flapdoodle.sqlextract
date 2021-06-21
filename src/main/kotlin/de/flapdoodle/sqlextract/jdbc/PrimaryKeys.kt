package de.flapdoodle.sqlextract.jdbc

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.db.PrimaryKey
import java.sql.DatabaseMetaData

object PrimaryKeys {
    fun primaryKeys(metaData: DatabaseMetaData, table: Name): Set<PrimaryKey> {
        return metaData.query { getPrimaryKeys(null, table.schema, table.name) }
            .map {
                val primaryKeyColumnName = expectColumn("COLUMN_NAME", String::class)
                val primaryKeyName = expectColumn("PK_NAME", String::class)
                //                println("PK -> $primaryKeyColumnName - $primaryKeyName")

                PrimaryKey(primaryKeyColumnName, primaryKeyName)
            }.toSet()
    }
}