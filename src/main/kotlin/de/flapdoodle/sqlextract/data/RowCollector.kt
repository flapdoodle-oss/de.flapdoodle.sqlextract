package de.flapdoodle.sqlextract.data

import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.db.Table

class RowCollector {
    private var tableRowsMap = emptyMap<Name, TableRows>()
    private var executedQueries = emptySet<QueryKey>()
    private var connections: Set<Pair<RowKey, RowKey>> = emptySet()

    fun rowMap(): List<TableRow> {
        return tableRowsMap.entries.flatMap {
            it.value.rows()
        }
    }

    fun add(rows: List<TableRow>, cause: RowKey?, direction: Direction) {
        rows.forEach { row ->
            entryFor(row.table.name)
                .add(row)
        }
        if (cause!=null) {
            when (direction) {
                Direction.FollowingForeignKeys -> {
                    connections = connections + rows.map { cause to it.key }
                }
                Direction.Backtrack -> {
                    connections = connections + rows.map { it.key to cause }
                }
            }
        }
    }

    fun connections() = connections

//    fun add(rows: List<TableRow>) {
//        rows.forEach { row ->
//            entryFor(row.table.name)
//                .add(row)
//        }
//    }
//
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

    private data class QueryKey(val query: String, val parameters: Map<Int, Any>)
}