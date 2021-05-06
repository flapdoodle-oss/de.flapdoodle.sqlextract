package de.flapdoodle.sqlextract.db

import java.sql.JDBCType

data class Column(
    val name: String,
    val dataType: JDBCType
) {
}