package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.jdbc.tables
import java.sql.Connection

class JdbcTableNamesFactory : TableNamesFactory {
    override fun tableNames(connection: Connection): List<Name> {
        return connection.metaData.tables().map { it.name }
    }
}