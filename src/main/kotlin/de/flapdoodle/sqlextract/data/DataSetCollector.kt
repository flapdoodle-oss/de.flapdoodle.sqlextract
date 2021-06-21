package de.flapdoodle.sqlextract.data

import de.flapdoodle.sqlextract.config.Backtrack
import de.flapdoodle.sqlextract.config.DataSet
import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.db.Table
import de.flapdoodle.sqlextract.db.TableListRepository
import de.flapdoodle.sqlextract.db.TableSet
import de.flapdoodle.sqlextract.graph.ForeignKeyAndReferenceGraph
import de.flapdoodle.sqlextract.jdbc.andCloseAfterUse
import de.flapdoodle.sqlextract.jdbc.query
import java.sql.Connection

class DataSetCollector(
    val connection: Connection,
    val tableSet: TableSet
) {
    private val tableGraph = ForeignKeyAndReferenceGraph.of(tableSet.all())
    private val rowCollector = RowCollector()
    private val tableRepository = TableListRepository(tableSet.all())

    fun collect(dataSet: DataSet) {
        val invalidBacktracks = dataSet.backtrack.filter {
            !tableGraph.isConnected(it.source, it.destination)
        }

        require(invalidBacktracks.isEmpty()) {
            "backtracks not valid: $invalidBacktracks"
        }

        val table = tableRepository.get(dataSet.table)
        val backtrackOverride = BacktrackOverride(dataSet.backtrack)

        val sqlQuery = selectQuery(dataSet.table, dataSet.where, dataSet.orderBy)
        collect(
            table,
            backtrackOverride,
            Query(sqlQuery),
            Strategy(CollectionMode.ForeignKeysAndBacktrack, Direction.FollowingForeignKeys),
            null,
        )
    }

    fun snapshot(): Snapshot {
        return Snapshot(tableGraph, rowCollector.rowMap(), rowCollector.connections())
    }

    private fun collect(
        table: Table,
        backtrackOverride: BacktrackOverride,
        query: Query,
        strategy: Strategy,
        cause: RowKey?
    ) {
        println("---------------------------------------------")
        println("collect ${query.sql} with ${query.parameters} (limit=${query.limit})")

        val newRows = connection.query {
            val statement = prepareStatement(query.sql)
            query.parameters.forEach { index, value ->
                statement.setObject(index, value)
            }
            if (query.limit != null) {
                statement.fetchSize = query.limit.toInt()
            }
            statement.executeQuery().andCloseAfterUse(statement)
        }
            .map {
                TableRow.of(table, table.columns.map { column ->
                    column.name to this.column(column.name, Object::class)
                }.toMap())
            }

        if (newRows.isEmpty() && strategy.direction == Direction.FollowingForeignKeys) {
            //TODO reenable this check throw IllegalArgumentException("expected any result, but got nothing.")
        }

        val missingRows = rowCollector.missingRows(newRows)
        rowCollector.add(newRows, cause, strategy.direction)

        println("collect ${query.sql} with ${query.parameters} (limit=${query.limit}) -> ${newRows.size} (missing: ${missingRows.size})")

        if (missingRows.isNotEmpty()) {
            val sourceTables = tableGraph.foreignKeysTo(table.name)
            val sourceReferenceTables = tableGraph.referencesTo(table.name)

            val validFKBacktrackTables = sourceTables.filter {
                strategy.direction == Direction.Backtrack
                        || strategy.mode == CollectionMode.ForeignKeysAndBacktrack
                        || backtrackOverride.find(it, table.name) != null
            }

            val validRefBacktrackTables = sourceReferenceTables.filter {
                backtrackOverride.find(it, table.name) != null
            }

            val validBacktrackTables = validFKBacktrackTables + validRefBacktrackTables.toSet()

            validBacktrackTables.forEach { from ->
                val fromTable = tableRepository.get(from)
                collect(
                    fromTable,
                    backtrackOverride.find(from, table.name),
                    backtrackOverride,
                    missingRows,
                    strategy.copy(direction = Direction.Backtrack)
                ) { row -> constraintsOf(fromTable, row) }
            }

            val tablesPointingTo = tableGraph.foreignKeysFrom(table.name)
            tablesPointingTo.forEach { to ->
                val toTable = tableRepository.get(to)
                collect(
                    toTable,
                    null,
                    backtrackOverride,
                    missingRows,
                    Strategy(CollectionMode.OnlyForeignKeys, Direction.FollowingForeignKeys)
                ) { row ->
                    constraintsOf(row, toTable)
                }
            }
        }
    }

    private fun collect(
        table: Table,
        tableConstraint: Backtrack?,
        backtrackOverride: BacktrackOverride,
        newRows: List<TableRow>,
        strategy: Strategy,
        constraintsFactory: (TableRow) -> List<Pair<String, Any?>>,
    ) {
        newRows.forEach { row ->
            val constraints = constraintsFactory(row)
            val constraintsSql = constraints.map { it.first } + (tableConstraint?.where ?: emptyList())
            val orderBy = tableConstraint?.orderBy ?: emptyList()

            val query = selectQuery(table.name, constraintsSql, orderBy)
            val parameters = constraints
                .filter { it.second != null }
                .mapIndexed { index, pair -> index + 1 to pair.second!! }
                .toMap()

            collect(table, backtrackOverride, Query(query, parameters, tableConstraint?.limit), strategy, row.key)
        }
    }

    private fun selectQuery(
        table: Name,
        where: List<String> = emptyList(),
        orderBy: List<String> = emptyList()
    ): String {
        val whereSQL = if (where.isNotEmpty())
            " where ${where.joinToString(" and ")}"
        else
            ""

        val orderBySQL = if (orderBy.isNotEmpty())
            " order by ${orderBy.joinToString(separator = ", ")}"
        else
            ""

        return "select * from ${table.asSQL()}$whereSQL$orderBySQL"
    }

    private fun constraintsOf(from: Table, to: TableRow): List<Pair<String, Any?>> {
        val fk = from.foreignKeys.filter { it.destinationTable == to.table.name }
        val ref = from.references.filter { it.destinationTable == to.table.name }
        val all = (fk + ref).toSet()
        require(all.isNotEmpty()) { "expected connections to ${to.table} from ${from.name}" }
        return all.map {
            val value = to.values[it.destinationColumn]
            if (value != null) {
                "${it.sourceColumn} = ?" to value
            } else {
                "${it.sourceColumn} is null" to null
            }
        }
    }

    private fun constraintsOf(from: TableRow, to: Table): List<Pair<String, Any?>> {
        val fk = from.table.foreignKeys.filter { it.destinationTable == to.name }
        val ref = from.table.references.filter { it.destinationTable == to.name }
        val all = (fk + ref).toSet()
        require(all.isNotEmpty()) { "expected foreign keys to ${from.table} from ${to.name}" }
        return all.map {
            val value = from.values[it.sourceColumn]
            if (value != null) {
                "${it.destinationColumn} = ?" to value
            } else {
                "${it.destinationColumn} is null" to null
            }
        }
    }

    private class BacktrackOverride(val backtrack: List<Backtrack>) {
        fun find(source: Name, destination: Name) = backtrack
            .filter { it.source == source && it.destination == destination }
            .singleOrNull()
    }

    private class Query(
        val sql: String,
        val parameters: Map<Int, Any> = emptyMap(),
        val limit: Long? = null,
    )

    private data class Strategy(
        val mode: CollectionMode,
        val direction: Direction
    )

    private enum class CollectionMode {
        ForeignKeysAndBacktrack,
        OnlyForeignKeys
    }

}