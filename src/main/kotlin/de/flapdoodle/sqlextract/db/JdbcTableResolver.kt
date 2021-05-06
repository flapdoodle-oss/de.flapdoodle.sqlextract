package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.jdbc.map
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.JDBCType
import java.sql.ResultSet
import javax.sql.rowset.JdbcRowSet


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

        metaData.map { getColumns(null, null, tableName, null) }
                .map {
                    val columnName = expectColumn("COLUMN_NAME", String::class)
                    val datatype = expectColumn("DATA_TYPE", BigDecimal::class).intValueExact()
//                    val columnsize = columns.getString("COLUMN_SIZE")
//                    val decimaldigits = columns.getString("DECIMAL_DIGITS")
//                    val isNullable = columns.getString("IS_NULLABLE")
//                    val is_autoIncrment = columns.getString("IS_AUTOINCREMENT")

                    println("$columnName -> ${JDBCType.valueOf(datatype)}")
                }

//        val columns = metaData.getColumns(null, null, tableName, null)
//        columns.use {
//            while (columns.next()) {
//                val columnName = columns.getString("COLUMN_NAME")
//                val datatype = columns.getInt("DATA_TYPE")
//                val columnsize = columns.getString("COLUMN_SIZE")
//                val decimaldigits = columns.getString("DECIMAL_DIGITS")
//                val isNullable = columns.getString("IS_NULLABLE")
//                val is_autoIncrment = columns.getString("IS_AUTOINCREMENT")
//
//                println("$columnName -> ${JDBCType.valueOf(datatype)}")
//            }
//        }

        val primaryKeys: ResultSet = metaData.getPrimaryKeys(null, null, name)
        primaryKeys.use {
            while (primaryKeys.next()) {
                val primaryKeyColumnName = primaryKeys.getString("COLUMN_NAME")
                val primaryKeyName = primaryKeys.getString("PK_NAME")
                println("PK -> $primaryKeyColumnName - $primaryKeyName")
            }
        }

        val foreignKeys: ResultSet = metaData.getImportedKeys(null, null, name)
        foreignKeys.use {
            while (foreignKeys.next()) {
                val pkTableName = foreignKeys.getString("PKTABLE_NAME")
                val fkTableName = foreignKeys.getString("FKTABLE_NAME")
                val pkColumnName = foreignKeys.getString("PKCOLUMN_NAME")
                var fkColumnName: String? = foreignKeys.getString("FKCOLUMN_NAME")
                println("FK -> $pkTableName:$pkColumnName <- $fkTableName:$fkColumnName")
            }
        }

        return null;
    }
}