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
            sqlQuery,
            emptyMap(),
            CollectionMode.ForeignKeysAndBacktrack,
            Direction.FollowingForeignKeys,
            backtrackOverride,
            null
        )
    }

    fun snapshot(): Snapshot {
        return Snapshot(tableGraph, rowCollector.rowMap())
    }

    private fun collect(
        table: Table,
        query: String,
        parameters: Map<Int, Any> = emptyMap(),
        mode: CollectionMode,
        direction: Direction,
        backtrackOverride: BacktrackOverride,
        limit: Long?,
    ) {
        if (!rowCollector.skipQuery(query, parameters)) {

            println("---------------------------------------------")
            println("collect $query with $parameters (limit=$limit)")

            val newRows = connection.query {
                val statement = prepareStatement(query)
                parameters.forEach { index, value ->
                    statement.setObject(index, value)
                }
                if (limit != null) {
                    statement.fetchSize = limit.toInt()
                }
                statement.executeQuery().andCloseAfterUse(statement)
            }
                .map {
                    TableRow.of(table, table.columns.map { column ->
                        column.name to this.column(column.name, Object::class)
                    }.toMap())
                }

            if (newRows.isEmpty() && direction==Direction.FollowingForeignKeys) {
                //TODO reenable this check throw IllegalArgumentException("expected any result, but got nothing.")
            }

            val missingRows = rowCollector.missingRows(newRows)
            rowCollector.add(newRows)

            println("collect $query with $parameters (limit=$limit) -> ${newRows.size} (missing: ${missingRows.size})")

            if (missingRows.isNotEmpty()) {
                val sourceTables = tableGraph.foreignKeysTo(table.name)
                val sourceReferenceTables = tableGraph.referencesTo(table.name)

                val validFKBacktrackTables = sourceTables.filter {
                    direction==Direction.Backtrack
                            || mode==CollectionMode.ForeignKeysAndBacktrack
                            || backtrackOverride.find(it, table.name)!=null
                }

                val validRefBacktrackTables = sourceReferenceTables.filter {
                    backtrackOverride.find(it, table.name)!=null
                }

                val validBacktrackTables=validFKBacktrackTables+validRefBacktrackTables.toSet()

                validBacktrackTables.forEach { from ->
                    val fromTable = tableRepository.get(from)
                    collect(
                        fromTable,
                        backtrackOverride.find(from, table.name),
                        backtrackOverride,
                        missingRows,
                        mode,
                        Direction.Backtrack
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
                        CollectionMode.OnlyForeignKeys,
                        Direction.FollowingForeignKeys
                    ) { row ->
                        constraintsOf(row, toTable)
                    }
                }
            }
        } else {
            println("---------------------------------------------")
            println("skip $query with $parameters (limit=$limit)")
        }
    }

    private fun collect(
        table: Table,
        tableConstraint: Backtrack?,
        backtrackOverride: BacktrackOverride,
        newRows: List<TableRow>,
        mode: CollectionMode,
        direction: Direction,
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

            collect(table, query, parameters, mode, direction, backtrackOverride, tableConstraint?.limit)
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
        val all = (fk+ref).toSet()
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
        val all = (fk+ref).toSet()
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
            .filter { it.source==source && it.destination==destination }
            .singleOrNull()
    }

    private enum class Direction {
        FollowingForeignKeys,
        Backtrack
    }

    private enum class CollectionMode {
        ForeignKeysAndBacktrack,
        OnlyForeignKeys
    }

    private class RowCollector {
        private var tableRowsMap = emptyMap<Name, TableRows>()
        private var executedQueries = emptySet<QueryKey>()

        fun rowMap(): Map<Table, List<Snapshot.Row>> {
            return tableRowsMap.entries.flatMap {
                it.value.rows()
            }.groupBy { it.table }
                .mapValues {
                    it.value.map { row -> Snapshot.Row(row.values) }
                }
        }

        fun add(rows: List<TableRow>) {
            rows.forEach { row ->
                entryFor(row.table.name)
                    .add(row)
            }
        }

        private fun entryFor(table: Name): TableRows {
            val ret = tableRowsMap[table]
            return if (ret == null) {
                val rows = TableRows(table)
                tableRowsMap = tableRowsMap + (table to rows)
                rows
            } else {
                ret
            }
        }

        fun missingRows(rows: List<TableRow>): List<TableRow> {
            return rows.filter { row ->
                !entryFor(row.table.name).contains(row)
            }
        }

        fun skipQuery(query: String, parameters: Map<Int, Any>): Boolean {
            val key = QueryKey(query, parameters)
            return if (!executedQueries.contains(key)) {
                executedQueries = executedQueries + key
                false
            } else
                true
        }

    }

    private data class QueryKey(val query: String, val parameters: Map<Int, Any>)

    private class TableRows(val table: Name) {
        private var rowMap = emptyMap<RowKey, TableRow>()

        fun add(row: TableRow) {
            require(row.table.name == table) { "wrong table: $row != $table" }
            if (!rowMap.containsKey(row.key)) {
                rowMap = rowMap + (row.key to row)
            }
        }

        fun contains(row: TableRow): Boolean {
            return rowMap.containsKey(row.key)
        }

        fun rows() = rowMap.values
    }

    private data class TableRow(val table: Table, val key: RowKey, val values: Map<String, Any?>) {

        companion object {
            fun of(table: Table, values: Map<String, Any?>): TableRow {
                return TableRow(table, rowKey(table, values), values)
            }

            private fun rowKey(table: Table, values: Map<String, Any?>): RowKey {
                require(table.primaryKeys.isNotEmpty()) {"table ${table.name} does not have any primary keys"}
                return RowKey(table, table.primaryKeys.map {
                    it.columnName to knownType(values[it.columnName])
                }.toMap())
            }

            private fun <T> knownType(value: T?): T? {
                return value
            }
        }
    }

}