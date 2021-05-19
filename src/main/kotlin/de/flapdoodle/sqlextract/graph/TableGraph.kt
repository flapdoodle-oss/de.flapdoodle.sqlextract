package de.flapdoodle.sqlextract.graph

import de.flapdoodle.graph.Graphs
import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.db.Table
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge

class TableGraph(
    val tables: List<Table>
) {
    private var graph: DefaultDirectedGraph<TableColumn, DefaultEdge>

    init {
        val tablesByName = tables.associateBy { it.name }
        val wrapper = Wrapper()
        tables.forEach { table ->
            table.foreignKeys.forEach { foreignKey ->
                val src = TableColumn(table.name, foreignKey.sourceColumn)
                val dstTable = tablesByName[Name(foreignKey.destinationTable, table.name.schema)]
                require(dstTable!=null) {"table ${foreignKey.destinationTable} not found"}
                val dst = TableColumn(dstTable.name, foreignKey.destinationColumn)
                wrapper.add(src,dst)
            }
        }
        this.graph = wrapper.build()
    }

    companion object {
        class Wrapper {
            private val builder = Graphs.graphBuilder(Graphs.directedGraph<TableColumn>()).get()

            fun add(source: TableColumn, dest: TableColumn) {
                builder.addVertex(source)
                builder.addVertex(dest)
                builder.addEdge(source, dest)
            }

            fun build(): DefaultDirectedGraph<TableColumn, DefaultEdge> {
                return builder.build()
            }
        }
    }
}