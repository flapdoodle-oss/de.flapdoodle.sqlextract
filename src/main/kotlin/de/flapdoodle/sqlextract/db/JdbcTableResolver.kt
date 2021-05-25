package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.io.Monitor
import de.flapdoodle.sqlextract.jdbc.query
import de.flapdoodle.sqlextract.jdbc.table
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.JDBCType


class JdbcTableResolver(
    connection: Connection,
//    private val postProcess: (Table) -> Table = { it }
) : TableResolver {

    private val metaData: DatabaseMetaData = connection.metaData

    override fun byName(name: Name): Table {
//        Monitor.message("")
        val table = metaData.table(name.name, name.schema)

        val columns = metaData.query { getColumns(null, table.name.schema, table.name.name, null) }
            .map {
                val columnName = expectColumn("COLUMN_NAME", String::class)
                val datatype = expectColumn("DATA_TYPE", Int::class)
                //                    val columnsize = columns.getString("COLUMN_SIZE")
                //                    val decimaldigits = columns.getString("DECIMAL_DIGITS")
                val isNullable = expectColumn("IS_NULLABLE", String::class).toLowerCase() == "yes"
                //                    val is_autoIncrment = columns.getString("IS_AUTOINCREMENT")

                //                    println("$columnName -> ${JDBCType.valueOf(datatype)}")

                Column(columnName, JDBCType.valueOf(datatype), isNullable)
            }.toSet()

        val primaryKeys = metaData.query { getPrimaryKeys(null, table.name.schema, table.name.name) }
            .map {
                val primaryKeyColumnName = expectColumn("COLUMN_NAME", String::class)
                val primaryKeyName = expectColumn("PK_NAME", String::class)
                //                println("PK -> $primaryKeyColumnName - $primaryKeyName")

                PrimaryKey(primaryKeyColumnName, primaryKeyName)
            }.toSet()


        val foreignKeys = metaData.query { getImportedKeys(null, table.name.schema, table.name.name) }
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


        //return postProcess(
        return Table(
                name = table.name,
                columns = columns,
                primaryKeys = primaryKeys,
                foreignKeys = foreignKeys
            )
        //)
    }
}