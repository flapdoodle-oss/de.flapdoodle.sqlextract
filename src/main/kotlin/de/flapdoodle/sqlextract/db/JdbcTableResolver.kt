package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.jdbc.query
import de.flapdoodle.sqlextract.jdbc.table
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.JDBCType


class JdbcTableResolver(
        connection: Connection,
        private val postProcess: (Table) -> Table = { it }
) : TableResolver {

    private val metaData: DatabaseMetaData = connection.metaData

//    ResultSet schemas = databaseMetaData.getSchemas();
//    while (schemas.next()){
//        String table_schem = schemas.getString("TABLE_SCHEM");
//        String table_catalog = schemas.getString("TABLE_CATALOG");
//    }

    override fun byName(name: Name): Table {
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


        val foreignKeys = metaData.query { getImportedKeys(null, table.name.schema, table.name.name)  }
            .map {
                val pkTableName = expectColumn("PKTABLE_NAME", String::class)
                val fkTableName = expectColumn("FKTABLE_NAME", String::class)
                val pkColumnName = expectColumn("PKCOLUMN_NAME", String::class)
                val fkColumnName = expectColumn("FKCOLUMN_NAME", String::class)
//                println("FK -> $pkTableName:$pkColumnName <- $fkTableName:$fkColumnName")

                ForeignKey(
                    sourceTable = fkTableName,
                    sourceColumn = fkColumnName,
                    destinationTable = pkTableName,
                    destinationColumn = pkColumnName
                )
            }.toSet()


        return postProcess(
            Table(
                name = table.name,
                columns = columns,
                primaryKeys = primaryKeys,
                foreignKeys = foreignKeys
            ))
    }
}