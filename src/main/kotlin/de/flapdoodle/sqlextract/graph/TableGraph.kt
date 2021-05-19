package de.flapdoodle.sqlextract.graph

import de.flapdoodle.graph.GraphAsDot
import de.flapdoodle.graph.Graphs
import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.db.Table
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge
import java.util.function.Predicate

class TableGraph(
    val graph: DefaultDirectedGraph<GraphVertex, DefaultEdge>
) {
    fun asDot(): String {
        return asDot(graph)
    }

    @Deprecated("not usefull")
    fun graphFor(table: Name): TableGraph {
        val current = GraphVertex.Table(table)

        val result: DefaultDirectedGraph<GraphVertex, DefaultEdge> = Graphs.filter(graph, Predicate {
            it == current || Graphs.hasPath(graph, current, it) || Graphs.hasPath(graph, it, current)
        })

        return TableGraph(result)
    }

    fun x(table: Name) {
        val current = GraphVertex.Table(table)

        val allRoots = Graphs.rootsOf(graph).firstOrNull()?.vertices() ?: emptySet()
        val connectedRoots = allRoots.filter {
            Graphs.hasPath(graph,it,current)
        }
        println("roots")
        println("---------------")
        connectedRoots.forEach {
            println("-> $it")
        }
        println("---------------")

        val filteredGraph = Graphs.filter(graph, Predicate {
            connectedRoots.contains(it) || connectedRoots.any { root -> Graphs.hasPath(graph, root, it) }
        })

        println(asDot(filteredGraph))

        val traversedGraph = Graphs.rootsOf(filteredGraph)

        println("traverse")
        println("---------------")
        traversedGraph.forEach {
            if (it.loops().isNotEmpty()) {
                println("looops!!!")
            }
            println("-> $it")
        }
        println("---------------")
    }

    companion object {

        private fun asDot(graph: DefaultDirectedGraph<GraphVertex, DefaultEdge>): String {
            val dotContent = GraphAsDot.builder<GraphVertex> { it.asId() }
                .nodeAttributes { mapOf("label" to it.simpleName()) }
                .label("tablegraph")
                .build()
                .asDot(graph)
            return dotContent
        }

        fun of(tables: List<Table>): TableGraph {
            val tablesByName = tables.associateBy { it.name }
            val wrapper = Wrapper()
            tables.forEach { table ->
                table.foreignKeys.forEach { foreignKey ->
                    val dstTable = tablesByName[Name(foreignKey.destinationTable, table.name.schema)]
                    require(dstTable!=null) {"table ${foreignKey.destinationTable} not found"}

                    wrapper.add(table.name, foreignKey.sourceColumn, dstTable.name, foreignKey.destinationColumn)
                }
            }

            return TableGraph(wrapper.build())
        }

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