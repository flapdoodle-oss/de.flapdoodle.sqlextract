package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.config.ForeignKeys
import de.flapdoodle.sqlextract.config.TableFilterList
import java.sql.Connection

class TableFromForeignKeyPathRepositoryFactory(
    private val tableNamesFactory: TableNamesFactory = JdbcTableNamesFactory(),
    private val tableResolverFactory: (Connection) -> TableResolver = { JdbcTableResolver(it) }
) : TableRepositoryFactory {
    override fun read(
        connection: Connection,
        tableFilter: TableFilterList,
        foreignKeys: List<ForeignKeys>
    ): Tables {
        val tableResolver = CachingTableResolverWrapper(
            tableResolverFactory(connection)
        )
            .withPostProcess(addForeignKeys(foreignKeys))
            .withMonitor()

        val tableNames = tableNamesFactory.tableNames(connection)

        val includedTables = tableNames.filter(tableFilter::matchingTableName)

        return Tables.empty().add(includedTables, tableResolver)
    }

    private fun addForeignKeys(foreignKeys: List<ForeignKeys>): (Table) -> Table {
        return {
            val keys = foreignKeys.filter { fk -> fk.schema == it.name.schema }
                .flatMap { fk -> fk.foreignKeys(it.name) }

            it.withForeignKeys(keys)
        }
    }

}