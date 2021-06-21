package de.flapdoodle.sqlextract.jdbc

import de.flapdoodle.sqlextract.db.Column
import de.flapdoodle.sqlextract.db.Name
import java.sql.DatabaseMetaData
import java.sql.JDBCType

object Columns {
    fun columns(metaData: DatabaseMetaData, table: Name): Set<Column> {
        val columns = metaData.query { getColumns(null, table.schema, table.name, null) }
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
        return columns
    }
}