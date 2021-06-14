package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.config.ForeignKeys
import de.flapdoodle.sqlextract.config.PrimaryKeys
import de.flapdoodle.sqlextract.config.TableFilterList
import de.flapdoodle.sqlextract.data.Target
import java.sql.Connection

class TableFromForeignKeyPathRepositoryFactory(
    private val tableNamesFactory: TableNamesFactory = JdbcTableNamesFactory(),
    private val tableResolverFactory: (Connection) -> TableResolver = { JdbcTableResolver(it) }
) : TableRepositoryFactory {
    override fun read(
        connection: Connection,
        tableFilter: TableFilterList,
        foreignKeys: List<ForeignKeys>,
        primaryKeys: List<PrimaryKeys>,
        target: Target
    ): Tables {
        val tableResolver = CachingTableResolverWrapper(
            tableResolverFactory(connection)
        )
            .withPostProcess(join(addForeignKeys(foreignKeys),addPrimaryKeys(primaryKeys)))
            .withMonitor()

        val tableNames = tableNamesFactory.tableNames(connection)

        val includedTables = tableNames.filter(tableFilter::matchingTableName)

        return Tables.empty().add(includedTables, tableResolver)
    }

    private fun join(first: (Table) -> Table, second: (Table) -> Table): (Table) -> Table {
        return {
            second(first(it))
        }
    }
    private fun addForeignKeys(foreignKeys: List<ForeignKeys>): (Table) -> Table {
        return {
            val keys = foreignKeys.filter { fk -> fk.schema == it.name.schema }
                .flatMap { fk -> fk.foreignKeys(it.name) }

            it.withForeignKeys(keys)
        }
    }

    private fun addPrimaryKeys(primaryKeys: List<PrimaryKeys>): (Table) -> Table {
        return {
            val keys = primaryKeys.filter { pk -> pk.schema == it.name.schema }
                .flatMap { fk -> fk.primaryKeys(it.name) }

            it.withPrimaryKeys(keys)
        }
    }
}