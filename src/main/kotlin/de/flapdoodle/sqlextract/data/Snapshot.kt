package de.flapdoodle.sqlextract.data

import de.flapdoodle.sqlextract.db.Table
import de.flapdoodle.sqlextract.graph.ForeignKeyGraph

data class Snapshot(
    val tableGraph: ForeignKeyGraph,
    val tableMap: Map<Table, List<Row>>
) {
    private val rowsByTableName = tableMap.mapKeys { it.key.name }
    private val tableByName = tableMap.keys.associateBy { it.name }

    data class Row(val values: Map<String, Any?>)

    fun insertSQL(): List<String> {
        val tablesInInsertOrder = tableGraph.tablesInInsertOrder()
        return tablesInInsertOrder.flatMap {
            val table = tableByName[it]
            val rows = rowsByTableName[it]
            // no data
            if (table != null && rows != null) {
                insertSQL(table, rows)
            } else {
                emptyList<String>()
            }
        }
    }

    fun tableGraphAsDot(): String {
        return tableGraph.filter(tableByName.keys).asDot()
    }

    private fun insertSQL(table: Table, rows: List<Row>): List<String> {
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
}
