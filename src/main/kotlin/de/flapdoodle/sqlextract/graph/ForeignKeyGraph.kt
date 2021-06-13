package de.flapdoodle.sqlextract.graph

import de.flapdoodle.graph.GraphAsDot
import de.flapdoodle.graph.Graphs
import de.flapdoodle.sqlextract.db.ForeignKey
import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.db.Table
import org.jgrapht.graph.DefaultDirectedGraph

class ForeignKeyGraph(
    val graph: DefaultDirectedGraph<GraphVertex, Edge>
) {

    fun asDot(): String {
        return asDot(graph)
    }

    fun dumpDebugInfo(table: Name) {
        println("--$table-8<---------------------------")
        val current = GraphVertex.Table(table)
        graph.incomingEdgesOf(current).forEach {
            println("$it -> ")
        }
        graph.outgoingEdgesOf(current).forEach {
            println("-> $it")
        }
        println("--$table->8---------------------------")
    }

    private fun foreignKeys(table: Name, incoming: Boolean): List<ForeignKey> {
        val current = GraphVertex.Table(table)
        val foreignKeyEdges = if (incoming) {
            val targetColumns = graph.sourcesOf(current)
            targetColumns.flatMap { targetColumn ->
                graph.incomingEdgesOf(targetColumn).map { it as Edge.ForeignKeyBetweenColumn }
            }
        } else {
            val sourceColumns = graph.targetsOf(current)
            sourceColumns.flatMap { targetColumn ->
                graph.outgoingEdgesOf(targetColumn).map { it as Edge.ForeignKeyBetweenColumn }
            }
        }
        return foreignKeyEdges.map { it.foreignKey }
    }

    fun referencesTo(table: Name): Set<Name> {
        return foreignKeys(table, true).map { it.sourceTable }.toSet()
    }

    fun referencesFrom(table: Name): Set<Name> {
        return foreignKeys(table, false).map { it.destinationTable }.toSet()
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
        private fun asDot(graph: DefaultDirectedGraph<GraphVertex, Edge>): String {
            val dotContent = GraphAsDot.builder<GraphVertex> { it.asId() }
                .nodeAttributes { mapOf("label" to it.simpleName()) }
                .edgeAttributes { start, end -> mapOf("label" to start.simpleName() + " - " + end.simpleName()) }
                .label("tablegraph")
                .build()
                .asDot(graph)
            return dotContent
        }

        fun of(tables: List<Table>): ForeignKeyGraph {
            val tablesByName = tables.associateBy { it.name }
            val wrapper = Wrapper()
            tables.forEach { table ->
                wrapper.add(table.name)
                table.foreignKeys.forEach { foreignKey ->
                    val dstTable = tablesByName[foreignKey.destinationTable]
                    require(dstTable != null) { "table ${foreignKey.destinationTable} not found" }

                    wrapper.add(foreignKey)
                }
            }

            return ForeignKeyGraph(wrapper.build())
        }

        sealed class Edge {
            data class TableToColumn(val table: Name, val column: String) : Edge()
            data class ForeignKeyBetweenColumn(val foreignKey: ForeignKey) : Edge()
        }

        class Wrapper {
            private val builder = Graphs.graphBuilder(Graphs.directedGraph<GraphVertex, Edge>(Edge::class.java)).get()

            fun add(name: Name) {
                val vertex = GraphVertex.Table(name)
                builder.addVertex(vertex)
            }

            fun add(foreignKey: ForeignKey) {
                val srcTable = GraphVertex.Table(foreignKey.sourceTable)
                val srcColumn = GraphVertex.TableColumn(foreignKey.sourceTable, foreignKey.sourceColumn)
                val dstColumn = GraphVertex.TableColumn(foreignKey.destinationTable, foreignKey.destinationColumn)
                val dstTable = GraphVertex.Table(foreignKey.destinationTable)

                builder.addVertex(srcTable)
                builder.addVertex(srcColumn)
                builder.addVertex(dstColumn)
                builder.addVertex(dstTable)

                builder.addEdge(
                    srcTable,
                    srcColumn,
                    Edge.TableToColumn(foreignKey.sourceTable, foreignKey.sourceColumn)
                )
                builder.addEdge(srcColumn, dstColumn, Edge.ForeignKeyBetweenColumn(foreignKey))
                builder.addEdge(
                    dstColumn,
                    dstTable,
                    Edge.TableToColumn(foreignKey.destinationTable, foreignKey.destinationColumn)
                )
            }

            fun build(): DefaultDirectedGraph<GraphVertex, Edge> {
                return builder.build()
            }
        }
    }
}