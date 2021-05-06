package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.jdbc.map
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.JDBCType


class JdbcTableResolver(
        private val connection: Connection
) : TableResolver {

    private val metaData: DatabaseMetaData = connection.metaData

//    ResultSet schemas = databaseMetaData.getSchemas();
//    while (schemas.next()){
//        String table_schem = schemas.getString("TABLE_SCHEM");
//        String table_catalog = schemas.getString("TABLE_CATALOG");
//    }

    override fun byName(name: String): Table? {
        val (tableName, remarks) = metaData.map { getTables(null,null,name, arrayOf("TABLE")) }
                .get {
                    val tableName = expectColumn("TABLE_NAME", String::class)
                    val remarks = column("REMARKS", String::class)
                    println("table -> $name -> $tableName ($remarks)")
                    tableName to remarks
                }

        val columns = metaData.map { getColumns(null, null, tableName, null) }
                .map {
                    val columnName = expectColumn("COLUMN_NAME", String::class)
                    val datatype = expectColumn("DATA_TYPE", BigDecimal::class).intValueExact()
//                    val columnsize = columns.getString("COLUMN_SIZE")
//                    val decimaldigits = columns.getString("DECIMAL_DIGITS")
//                    val isNullable = columns.getString("IS_NULLABLE")
//                    val is_autoIncrment = columns.getString("IS_AUTOINCREMENT")

                    println("$columnName -> ${JDBCType.valueOf(datatype)}")

                    Column(columnName, JDBCType.valueOf(datatype))
                }

        val primaryKeys = metaData.map { getPrimaryKeys(null, null, tableName) }
            .map {
                val primaryKeyColumnName = expectColumn("COLUMN_NAME", String::class)
                val primaryKeyName = expectColumn("PK_NAME", String::class)
                println("PK -> $primaryKeyColumnName - $primaryKeyName")

                PrimaryKey(primaryKeyColumnName, primaryKeyName)
            }


        val foreignKeys = metaData.map { getImportedKeys(null, null, tableName)  }
            .map {
                val pkTableName = expectColumn("PKTABLE_NAME", String::class)
                val fkTableName = expectColumn("FKTABLE_NAME", String::class)
                val pkColumnName = expectColumn("PKCOLUMN_NAME", String::class)
                var fkColumnName = expectColumn("FKCOLUMN_NAME", String::class)
                println("FK -> $pkTableName:$pkColumnName <- $fkTableName:$fkColumnName")

                ForeignKey(pkTableName, pkColumnName, fkTableName, fkColumnName)
            }


        return Table(
            name = tableName,
            columns = columns,
            primaryKeys = primaryKeys,
            foreignKeys = foreignKeys
        )
    }
}