package de.flapdoodle.sqlextract

import de.flapdoodle.sqlextract.db.*
import java.sql.JDBCType

class TableBuilder(val name: String, val schema: String = "PUBLIC") {

    var columns = emptyList<Column>()
    var foreignKeys = emptyList<ForeignKey>()
    var primaryKeys = emptyList<PrimaryKey>()

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

    fun primaryKey(
        column: String,
        name: String
    ): TableBuilder {
        primaryKeys = primaryKeys + PrimaryKey(column, name)
        return this
    }

    fun build(): Table {
        return Table(
            name = Name(name, schema),
            columns = columns.toSet(),
            foreignKeys = foreignKeys.toSet(),
            primaryKeys = primaryKeys.toSet()
        )
    }

}