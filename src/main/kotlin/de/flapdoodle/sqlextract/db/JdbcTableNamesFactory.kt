package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.jdbc.TableNames
import java.sql.Connection

class JdbcTableNamesFactory : TableNamesFactory {
    override fun tableNames(connection: Connection): List<Name> {
        return TableNames.tables(connection.metaData)
    }
}