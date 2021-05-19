package de.flapdoodle.sqlextract.graph

import de.flapdoodle.graph.GraphAsDot
import de.flapdoodle.graph.Graphs
import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.db.Table
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge

class TableGraph(
    val tables: List<Table>
) {
    private var graph: DefaultDirectedGraph<GraphVertex, DefaultEdge>

    init {
        val tablesByName = tables.associateBy { it.name }
        val wrapper = Wrapper()
        tables.forEach { table ->
            table.foreignKeys.forEach { foreignKey ->
                val dstTable = tablesByName[Name(foreignKey.destinationTable, table.name.schema)]
                require(dstTable!=null) {"table ${foreignKey.destinationTable} not found"}

                wrapper.add(table.name, foreignKey.sourceColumn, dstTable.name, foreignKey.destinationColumn)
            }
        }
        this.graph = wrapper.build()
    }

    fun asDot(): String {
        val dotContent = GraphAsDot.builder<GraphVertex> { it.asId() }
            .nodeAttributes { mapOf("label" to it.simpleName()) }
            .label("tablegraph")
            .build()
            .asDot(graph)

//        println("-----------------")
//        println(dotContent)
//        println("-----------------")
//        val loops = Graphs.loopsOf(graph)
//
//        loops.forEach {
//            println("loop: $it")
//        }
        return dotContent
    }

    fun graphFor(table: Name) {
        
    }

    companion object {
        class Wrapper {
            private val builder = Graphs.graphBuilder(Graphs.directedGraph<GraphVertex>()).get()

            fun add(source: Name, sourceColumn: String, dest: Name, destColumn: String) {
                val srcTable = GraphVertex.Table(source)
                val srcColumn = GraphVertex.TableColumn(source,sourceColumn)
                val dstColumn = GraphVertex.TableColumn(dest,destColumn)
                val dstTable = GraphVertex.Table(dest)

                builder.addVertex(srcTable)
                builder.addVertex(srcColumn)
                builder.addVertex(dstColumn)
                builder.addVertex(dstTable)

                builder.addEdge(srcTable,srcColumn)
                builder.addEdge(srcColumn,dstColumn)
                builder.addEdge(dstColumn,dstTable)
            }

            fun build(): DefaultDirectedGraph<GraphVertex, DefaultEdge> {
                return builder.build()
            }
        }
    }
}