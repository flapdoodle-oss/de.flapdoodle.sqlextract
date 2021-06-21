package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.jdbc.JdbcTable
import java.sql.Connection

class JdbcTableNamesFactory : TableNamesFactory {
    override fun tableNames(connection: Connection): List<Name> {
        return JdbcTable.tables(connection.metaData).map { it.name }
    }
}