package de.flapdoodle.sqlextract.jdbc

import de.flapdoodle.sqlextract.db.ForeignKey
import de.flapdoodle.sqlextract.db.Name
import java.sql.DatabaseMetaData

object ForeignKeys {

    fun foreignKeys(metaData: DatabaseMetaData, table: Name): Set<ForeignKey> {
        return metaData.query { getImportedKeys(null, table.schema, table.name) }
            .map {
                val pkSchemaName = expectColumn(setOf("PKTABLE_SCHEMA","PKTABLE_SCHEM"), String::class)
                val fkSchemaName = expectColumn(setOf("FKTABLE_SCHEMA","FKTABLE_SCHEM"), String::class)
                val pkTableName = expectColumn("PKTABLE_NAME", String::class)
                val fkTableName = expectColumn("FKTABLE_NAME", String::class)
                val pkColumnName = expectColumn("PKCOLUMN_NAME", String::class)
                val fkColumnName = expectColumn("FKCOLUMN_NAME", String::class)
                //                println("FK -> $pkTableName:$pkColumnName <- $fkTableName:$fkColumnName")

                ForeignKey(
                    sourceTable = Name(fkTableName, fkSchemaName),
                    sourceColumn = fkColumnName,
                    destinationTable = Name(pkTableName, pkSchemaName),
                    destinationColumn = pkColumnName
                )
            }.toSet()
    }
}