package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.config.ForeignKeys
import de.flapdoodle.sqlextract.config.TableFilterList
import java.sql.Connection

interface TableRepositoryFactory {
    fun read(connection: Connection, tableFilter: TableFilterList, foreignKeys: List<ForeignKeys>): TableRepository
}