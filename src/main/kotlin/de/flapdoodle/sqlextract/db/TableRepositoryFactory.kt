package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.config.ForeignKeys
import de.flapdoodle.sqlextract.config.PrimaryKeys
import de.flapdoodle.sqlextract.config.References
import de.flapdoodle.sqlextract.config.TableFilterList
import de.flapdoodle.sqlextract.data.Target
import java.sql.Connection

fun interface TableRepositoryFactory {
    fun read(
        connection: Connection,
        tableFilter: TableFilterList,
        foreignKeys: List<ForeignKeys>,
        primaryKey: List<PrimaryKeys>,
        references: List<References>,
        target: Target
    ): TableSet
}