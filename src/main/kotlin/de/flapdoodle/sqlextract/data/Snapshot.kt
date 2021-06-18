package de.flapdoodle.sqlextract.data

import de.flapdoodle.sqlextract.cache.PersistedTables
import de.flapdoodle.sqlextract.db.Table
import de.flapdoodle.sqlextract.graph.ForeignKeyAndReferenceGraph

data class Snapshot(
    val tableGraph: ForeignKeyAndReferenceGraph,
    val tableRows: List<TableRow>,
    val rowConnections: Set<Pair<RowKey, RowKey>>
) {
    private val tableByName = tableRows.associate { it.table.name to it.table }
    private val rowsByTable = tableRows.groupBy { it.table }

    data class Row(val values: Map<String, Any?>)

    fun insertSQL(): List<String> {
        val tablesInInsertOrder = tableGraph.tablesInInsertOrder()
        return tablesInInsertOrder.flatMap {
            val table = tableByName[it]
            val rows = rowsByTable[table]
            // no data
            if (table != null && rows != null) {
                insertSQL(table, rows)
            } else {
                emptyList<String>()
            }
        }
    }

    fun schemaGraphAsDot(): String {
        return tableGraph.asDot()
    }

    fun schemaAsJson(): String {
        return PersistedTables.asJson(tableByName.values.toList(), "<no-hash>")
    }

    fun tableGraphAsDot(): String {
        return tableGraph.filter(tableByName.keys).asDot()
    }

    fun tableRowsAsDot(): String {
        return tableRowsVerticalAsDot(tableGraph, rowsByTable)
    }

    private fun insertSQL(table: Table, rows: List<TableRow>): List<String> {
        val stringBuilder = StringBuilder()
        stringBuilder
            .append("INSERT INTO ${table.name.asSQL()}\n")
            .append("(")
            .append(table.columns.map { it.name }.joinToString(separator = ", "))
            .append(")\n")
            .append(rows.map { row ->
                "(" + table.columns.map { column ->
                    val value = row.values[column.name]
                    require(column.nullable || value != null) { "value is null but column is not nullable: $column" }
                    asSql(value)
                }.joinToString(separator = ", ") + ")"
            }.joinToString(",\n"))

        return listOf(stringBuilder.toString())
    }

    private fun asSql(value: Any?): String {
        return if (value != null)
            when (value) {
                is String -> "'$value'"
                else -> value.toString()
            }
        else
            "null"
    }

    private fun tableRowsVerticalAsDot(
        tableGraph: ForeignKeyAndReferenceGraph,
        tableMap: Map<Table, List<TableRow>>
    ): String {
        val rowKey2Number: Map<RowKey, String> = tableMap.values
            .flatMap { list -> list.map { it.key } }
            .mapIndexed { index, rowKey -> rowKey to "row_$index" }
            .toMap()

        return "digraph structs {\n" +
                "node [shape=plaintext]\n" +
                "\n" +
                tablesVertical(tableMap, rowKey2Number) +
                "\n" +
//                connections(tableGraph, tableMap) +
//                "\n" +
                rowConnections(rowConnections, rowKey2Number) +
                "\n" +
                "\n}"
    }

    private fun rowConnections(
        rowConnections: Set<Pair<RowKey, RowKey>>,
        rowKey2Number: Map<RowKey, String>
    ): String {
        return rowConnections.map {
            "${it.first.table.name.asId()}:${rowKey2Number[it.first]} -> ${it.second.table.name.asId()}:${rowKey2Number[it.second]};"
        }.joinToString(separator = "\n")
    }

    private fun connections(
        tableGraph: ForeignKeyAndReferenceGraph,
        tableMap: Map<Table, List<TableRow>>
    ): String {
        val tableNames = tableMap.keys.map { it.name }
        return tableNames.flatMap {
            val fk = tableGraph.foreignKeys(it, true)
            val ref = tableGraph.references(it, true)
            val all = fk + ref
            all.filter { tableNames.contains(it.sourceTable) }
        }.map {
            "${it.sourceTable.asId()}:${it.sourceColumn} -> ${it.destinationTable.asId()}:${it.destinationColumn};"
        }.joinToString("\n")
    }

    private fun tablesVertical(tableMap: Map<Table, List<TableRow>>, rowKey2Number: Map<RowKey, String>): String {
        return tableMap.map { (table, rows) ->
            "\"${table.name.asId()}\" [label=<${rowsAsVerticalHtmlTable(table, rows, rowKey2Number)}>];"
        }.joinToString(separator = "\n\n")
    }

    private fun rowsAsVerticalHtmlTable(
        table: Table,
        rows: List<TableRow>,
        rowKey2Number: Map<RowKey, String>
    ): String {
        val header = "<TR><TD COLSPAN=\"${rows.size + 1}\">${table.name.asSQL()}</TD></TR>\n"

        val rowsAsHtml = table.columns.mapIndexed { index, col ->
            val rowAsHtml = rows.map { row ->
                val value = row.values[col.name]
                val valueAsSql = asSql(value)
                val portAttr = if (index == 0) " PORT=\"${rowKey2Number[row.key]}\"" else ""
                "<TD $portAttr>$valueAsSql</TD>"
            }.joinToString()
            "<TR><TD PORT=\"${col.name}\">${col.name}</TD>$rowAsHtml</TR>"
        }.joinToString(separator = "\n")

        return "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n" +
                header +
                rowsAsHtml +
                "\n</TABLE>"
    }
}
