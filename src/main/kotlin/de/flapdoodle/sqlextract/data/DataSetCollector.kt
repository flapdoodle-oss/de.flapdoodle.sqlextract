package de.flapdoodle.sqlextract.data

import de.flapdoodle.sqlextract.config.Constraint
import de.flapdoodle.sqlextract.config.DataSet
import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.db.Table
import de.flapdoodle.sqlextract.db.Tables
import de.flapdoodle.sqlextract.graph.TableGraph
import de.flapdoodle.sqlextract.jdbc.andCloseAfterUse
import de.flapdoodle.sqlextract.jdbc.query
import java.sql.Connection

class DataSetCollector(
    val connection: Connection,
    val tables: Tables
) {
    private val tableGraph = TableGraph.of(tables.all())
    private val rowCollector = RowCollector()

    fun collect(dataSet: DataSet) {
        val filteredGraph = tableGraph.filter(dataSet.table)
        val table = tables.get(dataSet.table)
        val constraints = TableConstraints(dataSet.constraints)

        val sqlQuery = selectQuery(dataSet.table, dataSet.where, dataSet.orderBy)
        collect(table, filteredGraph, constraints, sqlQuery)
    }

    fun snapshot(): Snapshot {
        return Snapshot(tableGraph, rowCollector.rowMap())
    }

    private fun collect(
        table: Table,
        filteredGraph: TableGraph,
        constraints: TableConstraints,
        query: String,
        parameters: Map<Int, Any> = emptyMap()
    ) {
        if (!rowCollector.skipQuery(query, parameters)) {

            val tableConstraint = constraints.find(table.name)

            println("---------------------------------------------")
            println("collect $query with $parameters ($tableConstraint)")

            val newRows = connection.query {
                val statement = prepareStatement(query)
                parameters.forEach { index, value ->
                    statement.setObject(index, value)
                }
                if (tableConstraint?.limit != null) {
                    statement.fetchSize = tableConstraint.limit.toInt()
                }
                statement.executeQuery().andCloseAfterUse(statement)
            }
                .map {
                    TableRow.of(table, table.columns.map { column ->
                        column.name to this.column(column.name, Object::class)
                    }.toMap())
                }

            val missingRows = rowCollector.missingRows(newRows)
            rowCollector.add(newRows)

            if (missingRows.isNotEmpty()) {
                val tablesPointingFrom = filteredGraph.referencesTo(table.name)
                tablesPointingFrom.forEach { from ->
                    val fromTable = tables.get(from)
                    collect(fromTable, filteredGraph, constraints, missingRows) { row -> constraintsOf(fromTable, row) }
                }
                val tablesPointingTo = filteredGraph.referencesFrom(table.name)
                tablesPointingTo.forEach { to ->
                    val toTable = tables.get(to)
                    collect(toTable, filteredGraph, constraints, missingRows) { row -> constraintsOf(row, toTable) }
                }
            }
        }
    }

    private fun collect(
        table: Table,
        filteredGraph: TableGraph,
        tableConstraints: TableConstraints,
        newRows: List<TableRow>,
        constraintsFactory: (TableRow) -> List<Pair<String, Any?>>
    ) {
        val tableConstraint = tableConstraints.find(table.name)

        newRows.forEach { row ->
            val constraints = constraintsFactory(row)
            val constraintsSql = constraints.map { it.first } + (tableConstraint?.where ?: emptyList())
            val orderBy = tableConstraint?.orderBy ?: emptyList()

            val query = selectQuery(table.name, constraintsSql, orderBy)
            val parameters = constraints
                .filter { it.second != null }
                .mapIndexed { index, pair -> index + 1 to pair.second!! }
                .toMap()

            collect(table, filteredGraph, tableConstraints, query, parameters)
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
        val fk = from.foreignKeys.filter { it.destinationTable == to.table.name.name }
        require(fk.isNotEmpty()) { "expected foreign keys to ${to.table} from ${from.name}" }
        return fk.map {
            val value = to.values[it.destinationColumn]
            if (value != null) {
                "${it.sourceColumn} = ?" to value
            } else {
                "${it.sourceColumn} is null" to null
            }
        }
    }

    private fun constraintsOf(from: TableRow, to: Table): List<Pair<String, Any?>> {
        val fk = from.table.foreignKeys.filter { it.destinationTable == to.name.name }
        require(fk.isNotEmpty()) { "expected foreign keys to ${from.table} from ${to.name}" }
        return fk.map {
            val value = from.values[it.sourceColumn]
            if (value != null) {
                "${it.destinationColumn} = ?" to value
            } else {
                "${it.destinationColumn} is null" to null
            }
        }
    }

    private class TableConstraints(constraints: List<Constraint>) {
        private val byTable = constraints.associateBy { it.table }

        fun find(table: Name) = byTable[table]
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
                return RowKey(table.primaryKeys.map {
                    it.columnName to knownType(values[it.columnName])
                }.toMap())
            }

            private fun <T> knownType(value: T?): T? {
                return value
            }
        }
    }

    private data class RowKey(val key: Map<String, Any?>)
}