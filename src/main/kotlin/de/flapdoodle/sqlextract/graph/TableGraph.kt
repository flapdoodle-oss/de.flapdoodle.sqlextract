package de.flapdoodle.sqlextract.graph

import de.flapdoodle.graph.GraphAsDot
import de.flapdoodle.graph.Graphs
import de.flapdoodle.graph.Loop
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

    fun referencesTo(table: Name): List<Name> {
        val current = GraphVertex.Table(table)

        val currentColumns = graph.sourcesOf(current)
        val sourceColumns = currentColumns.flatMap { graph.sourcesOf(it) }
        val sourceTables = sourceColumns.flatMap { graph.sourcesOf(it) }

        require(sourceTables.all { it is GraphVertex.Table }) { "wrong type: $sourceTables" }

        return sourceTables.map { it as GraphVertex.Table }.map { it.table }
    }

    fun referencesFrom(table: Name): List<Name> {
        val current = GraphVertex.Table(table)

        val currentColumns = graph.targetsOf(current)
        val targetColumns = currentColumns.flatMap { graph.targetsOf(it) }
        val targetTables = targetColumns.flatMap { graph.targetsOf(it) }

        require(targetTables.all { it is GraphVertex.Table }) { "wrong type: $targetTables" }

        return targetTables.map { it as GraphVertex.Table }.map { it.table }
    }

    fun filter(table: Name): TableGraph {
        val current = GraphVertex.Table(table)

        val allRoots = Graphs.rootsOf(graph).firstOrNull()?.vertices() ?: emptySet()
        val connectedRoots = allRoots.filter {
            Graphs.hasPath(graph, it, current)
        }
//        println("roots")
//        println("---------------")
//        connectedRoots.forEach {
//            println("-> $it")
//        }
//        println("---------------")

        val filteredGraph = Graphs.filter(graph, Predicate {
            connectedRoots.contains(it)
                    || connectedRoots.any { root -> Graphs.hasPath(graph, root, it) }
                    || it == current
        })

//        println(asDot(filteredGraph))
//
//        val traversedGraph = Graphs.rootsOf(filteredGraph)
//
//        println("traverse")
//        println("---------------")
//        traversedGraph.forEach {
//            if (it.loops().isNotEmpty()) {
//                println("looops!!!")
//            }
//            println("-> $it")
//        }
//        println("---------------")

        return TableGraph(filteredGraph)
    }

    fun tablesInInsertOrder(): List<Name> {
        val loops = Graphs.loopsOf(graph)
//        require(loops.isEmpty()) {"loops not supported: $loops"}

        val startingFromLeafs = Graphs.leavesOf(graph)
        return startingFromLeafs.flatMap { verticesAndEdges ->
            if (verticesAndEdges.loops().isNotEmpty()) {
                println("---------------------------------")
                println("warning: loops not implemented now")
                verticesAndEdges.loops().forEach { loop ->
                    println("parts: ${loop.vertexSet()}")
                    loop.edges().forEach { edge ->
                        println("${edge.start()} -> ${edge.end()}")
                    }
                }
                println("---------------------------------")
            }

            verticesAndEdges.vertices().filterIsInstance<GraphVertex.Table>().map { it.table }
        }
    }


    companion object {

        private fun asDot(graph: DefaultDirectedGraph<GraphVertex, DefaultEdge>): String {
            val dotContent = GraphAsDot.builder<GraphVertex> { it.asId() }
                .nodeAttributes { mapOf("label" to it.simpleName()) }
                .edgeAttributes { start, end -> mapOf("label" to start.simpleName() + " - " + end.simpleName()) }
                .label("tablegraph")
                .build()
                .asDot(graph)
            return dotContent
        }

        fun of(tables: List<Table>): TableGraph {
            val tablesByName = tables.associateBy { it.name }
            val wrapper = Wrapper()
            tables.forEach { table ->
                wrapper.add(table.name)
                table.foreignKeys.forEach { foreignKey ->
                    val dstTable = tablesByName[foreignKey.destinationTable]
                    require(dstTable != null) { "table ${foreignKey.destinationTable} not found" }

                    wrapper.add(table.name, foreignKey.sourceColumn, dstTable.name, foreignKey.destinationColumn)
                }
            }

            return TableGraph(wrapper.build())
        }

        class Wrapper {
            private val builder = Graphs.graphBuilder(Graphs.directedGraph<GraphVertex>()).get()

            fun add(name: Name) {
                val vertex = GraphVertex.Table(name)
                builder.addVertex(vertex)
            }

            fun add(source: Name, sourceColumn: String, dest: Name, destColumn: String) {
                val srcTable = GraphVertex.Table(source)
                val srcColumn = GraphVertex.TableColumn(source, sourceColumn)
                val dstColumn = GraphVertex.TableColumn(dest, destColumn)
                val dstTable = GraphVertex.Table(dest)

                builder.addVertex(srcTable)
                builder.addVertex(srcColumn)
                builder.addVertex(dstColumn)
                builder.addVertex(dstTable)

                builder.addEdge(srcTable, srcColumn)
                builder.addEdge(srcColumn, dstColumn)
                builder.addEdge(dstColumn, dstTable)
            }

            fun build(): DefaultDirectedGraph<GraphVertex, DefaultEdge> {
                return builder.build()
            }
        }
    }
}