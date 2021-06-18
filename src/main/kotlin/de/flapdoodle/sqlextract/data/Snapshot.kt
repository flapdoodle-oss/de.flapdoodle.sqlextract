package de.flapdoodle.sqlextract.data

import de.flapdoodle.sqlextract.cache.PersistedTables
import de.flapdoodle.sqlextract.db.Table
import de.flapdoodle.sqlextract.graph.ForeignKeyAndReferenceGraph

data class Snapshot(
    val tableGraph: ForeignKeyAndReferenceGraph,
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

    fun schemaGraphAsDot(): String {
        return tableGraph.asDot()
    }

    fun schemaAsJson(): String {
        return PersistedTables.asJson(tableMap.keys.toList(),"<no-hash>")
    }

    fun tableGraphAsDot(): String {
        return tableGraph.filter(tableByName.keys).asDot()
    }

    fun tableRowsAsDot(): String {
        return tableRowsVerticalAsDot(tableGraph, tableMap)
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

    /*
    digraph structs {
node [shape=plaintext]
struct1 [label=<
<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
<TR><TD colspan="3">FOO</TD></TR>
<TR><TD><b>left</b></TD><TD PORT="f1">mid dle</TD><TD PORT="f2">right</TD></TR>
</TABLE>>];
struct2 [label=<
<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
<TR><TD PORT="f0">one</TD><TD>two</TD></TR>
</TABLE>>];
struct3 [label=<
<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
<TR>
<TD ROWSPAN="3">hello<BR/>world</TD>
<TD COLSPAN="3">b</TD>
<TD ROWSPAN="3">g</TD>
<TD ROWSPAN="3">h</TD>
</TR>
<TR>
<TD>c</TD><TD PORT="here">d</TD><TD>e</TD>
</TR>
<TR>
<TD COLSPAN="3">f</TD>
</TR>
</TABLE>>];
struct1:f1 -> struct2:f0;
struct1:f2 -> struct3:here;
}
     */

    private fun tableRowsVerticalAsDot(
        tableGraph: ForeignKeyAndReferenceGraph,
        tableMap: Map<Table, List<Row>>
    ): String {
        return "digraph structs {\n" +
                "node [shape=plaintext]\n"+
                "\n"+
                tablesVertical(tableMap)+
                "\n"+
                connections(tableGraph, tableMap) +
                "\n}"
    }

    private fun tableRowsAsDot(
        tableGraph: ForeignKeyAndReferenceGraph,
        tableMap: Map<Table, List<Row>>
    ): String {
        return "digraph structs {\n" +
                "node [shape=plaintext]\n"+
                "\n"+
                tables(tableMap)+
                "\n"+
                connections(tableGraph, tableMap) +
                "\n}"
    }

    private fun connections(
        tableGraph: ForeignKeyAndReferenceGraph,
        tableMap: Map<Table, List<Row>>
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

    private fun tablesVertical(tableMap: Map<Table, List<Row>>): String {
        return tableMap.map { (table, rows) ->
            "\"${table.name.asId()}\" [label=<${rowsAsVerticalHtmlTable(table, rows)}>];"
        }.joinToString(separator = "\n\n")
    }

    private fun tables(tableMap: Map<Table, List<Row>>): String {
        return tableMap.map { (table, rows) ->
            "\"${table.name.asId()}\" [label=<${rowsAsHtmlTable(table, rows)}>];"
        }.joinToString(separator = "\n\n")
    }

    private fun rowsAsVerticalHtmlTable(table: Table, rows: List<Row>): String {
        val header="<TR><TD COLSPAN=\"${rows.size+1}\">${table.name.asSQL()}</TD></TR>\n"

        val rowsAsHtml = table.columns.map { col ->
            val rowAsHtml = rows.map { row ->
                val value = row.values[col.name]
                val valueAsSql = asSql(value)
                "<TD>$valueAsSql</TD>"
            }.joinToString()
            "<TR><TD PORT=\"${col.name}\">${col.name}</TD>$rowAsHtml</TR>"
        }.joinToString(separator = "\n")

        return "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n" +
                header +
                rowsAsHtml+
                "\n</TABLE>"
    }

    private fun rowsAsHtmlTable(table: Table, rows: List<Row>): String {
        val columnNames = table.columns.map {
            "<TD PORT=\"${it.name}\">${it.name}</TD>"
        }

        val header="<TR><TD COLSPAN=\"${columnNames.size}\">${table.name.asSQL()}</TD></TR>\n"

        val rowsAsHtml = rows.map { row ->
            val rowAsHtml = table.columns.map { col ->
                val value = row.values[col.name]
                val valueAsSql = asSql(value)
                "<TD>$valueAsSql</TD>"
            }.joinToString()

            "<TR>$rowAsHtml</TR>"
        }.joinToString(separator = "\n")

        return "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n" +
                header +
                "<TR>\n${columnNames.joinToString(separator = "\n")}\n</TR>\n"+
                rowsAsHtml+
                "\n</TABLE>"
    }
}
