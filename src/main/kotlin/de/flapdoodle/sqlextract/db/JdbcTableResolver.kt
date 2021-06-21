package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.jdbc.*
import java.sql.Connection
import java.sql.DatabaseMetaData


class JdbcTableResolver(
    connection: Connection,
) : TableResolver {

    private val metaData: DatabaseMetaData = connection.metaData

    override fun byName(name: Name): Table {
        val table = JdbcTable.table(metaData, name.name, name.schema)
        val columns = Columns.columns(metaData, table.name)
        val primaryKeys = PrimaryKeys.primaryKeys(metaData, table.name)
        val foreignKeys = ForeignKeys.foreignKeys(metaData, table.name)

        return Table(
            name = table.name,
            columns = columns,
            primaryKeys = primaryKeys,
            foreignKeys = foreignKeys
        )
    }
}