package de.flapdoodle.sqlextract

import de.flapdoodle.sqlextract.db.Column
import de.flapdoodle.sqlextract.db.ForeignKey
import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.db.Table
import java.sql.JDBCType

class TableBuilder(val name: String, val schema: String = "PUBLIC") {

    var columns = emptyList<Column>()
    var foreignKeys = emptyList<ForeignKey>()

    fun column(
        name: String,
        dataType: JDBCType,
        nullable: Boolean = false
    ): TableBuilder {
        columns = columns + Column(name, dataType, nullable)
        return this
    }

    fun foreignKey(
        sourceColumn: String,
        destinationTable: String,
        destinationColumn: String
    ): TableBuilder {
        foreignKeys = foreignKeys + ForeignKey(Name(name, schema), sourceColumn, Name(destinationTable, schema), destinationColumn)
        return this
    }

    fun build(): Table {
        return Table(
            name = Name(name, schema),
            columns = columns.toSet(),
            foreignKeys = foreignKeys.toSet()
        )
    }

}