package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.jdbc.*
import java.sql.Connection
import java.sql.DatabaseMetaData


class JdbcTableResolver(
    connection: Connection,
) : TableResolver {

    private val metaData: DatabaseMetaData = connection.metaData

    override fun byName(name: Name): Table {
        val columns = Columns.columns(metaData, name)
        val primaryKeys = PrimaryKeys.primaryKeys(metaData, name)
        val foreignKeys = ForeignKeys.foreignKeys(metaData, name)

        return Table(
            name = name,
            columns = columns,
            primaryKeys = primaryKeys,
            foreignKeys = foreignKeys
        )
    }
}