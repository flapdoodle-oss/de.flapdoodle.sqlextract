package de.flapdoodle.sqlextract.graph

import de.flapdoodle.graph.GraphAsDot
import de.flapdoodle.graph.Graphs
import de.flapdoodle.sqlextract.db.ForeignKey
import de.flapdoodle.sqlextract.db.Name
import de.flapdoodle.sqlextract.db.Reference
import de.flapdoodle.sqlextract.db.Table
import org.jgrapht.graph.DefaultDirectedGraph

class ForeignKeyAndReferenceGraph(
    val graph: DefaultDirectedGraph<Name, Edge>
) {

    fun asDot(): String {
        return asDot(graph)
    }

    fun filter(tables: Set<Name>): ForeignKeyAndReferenceGraph {
        return ForeignKeyAndReferenceGraph(Graphs.filter(graph) { current ->
            tables.any { table ->
                Graphs.hasPath(graph, table, current) || Graphs.hasPath(graph, current, table)
            }
        })
    }

    private fun edges(table: Name, incoming: Boolean): Set<Edge> {
        val edges = if (incoming) {
            graph.incomingEdgesOf(table)
        } else {
            graph.outgoingEdgesOf(table)
        }
        return edges
    }

    private fun foreignKeys(table: Name, incoming: Boolean): List<ForeignKey> {
        return edges(table, incoming).filterIsInstance<Edge.ForeignKeyBetweenColumn>()
            .map { it.foreignKey }
    }

    private fun references(table: Name, incoming: Boolean): List<Reference> {
        return edges(table, incoming).filterIsInstance<Edge.ReferenceBetweenColumn>()
            .map { it.reference }
    }

    fun foreignKeysTo(table: Name): Set<Name> {
        return foreignKeys(table, true).map { it.sourceTable }.toSet()
    }

    fun referencesTo(table: Name): Set<Name> {
        return references(table, true).map { it.sourceTable }.toSet()
    }

    fun foreignKeysFrom(table: Name): Set<Name> {
        return foreignKeys(table, false).map { it.destinationTable }.toSet()
    }

    fun isConnected(source: Name, destination: Name): Boolean {
        return foreignKeysTo(destination).contains(source) || referencesTo(destination).contains(source)
    }


    fun tablesInInsertOrder(): List<Name> {
//        val loops = Graphs.loopsOf(graph)
////        require(loops.isEmpty()) {"loops not supported: $loops"}

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

            verticesAndEdges.vertices().map { it }
        }
    }


    companion object {
        fun Name.asId(): String {
            return schema+"_"+name
        }

        fun Name.simpleName(): String {
            return schema+"."+name
        }

        private fun asDot(graph: DefaultDirectedGraph<Name, Edge>): String {
            val dotContent = GraphAsDot.builder<Name> { it.asId() }
                .nodeAttributes { mapOf("label" to it.simpleName()) }
                .edgeAttributes { start, end -> mapOf("label" to start.simpleName() + " - " + end.simpleName()) }
                .label("tablegraph")
                .build()
                .asDot(graph)
            return dotContent
        }

        fun of(tables: List<Table>): ForeignKeyAndReferenceGraph {
            val tablesByName = tables.associateBy { it.name }
            val wrapper = Wrapper()
            tables.forEach { table ->
                wrapper.add(table.name)
                table.foreignKeys.forEach { foreignKey ->
                    val dstTable = tablesByName[foreignKey.destinationTable]
                    require(dstTable != null) { "table ${foreignKey.destinationTable} not found" }
                    requireRoreignKeyMatchesPrimaryKey(dstTable,foreignKey)

                    wrapper.add(foreignKey)
                }
                table.references.forEach { ref ->
                    val dstTable = tablesByName[ref.destinationTable]
                    require(dstTable != null) { "table ${ref.destinationTable} not found" }

                    wrapper.add(ref)
                }
            }

            return ForeignKeyAndReferenceGraph(wrapper.build())
        }

        private fun requireRoreignKeyMatchesPrimaryKey(dstTable: Table, foreignKey: ForeignKey) {
            val matchingPK = dstTable.primaryKeys.filter { it.columnName==foreignKey.destinationColumn }
            require(matchingPK.isNotEmpty()) {"foreign key $foreignKey does not match primary keys (${dstTable.primaryKeys}) in ${dstTable.name}"}
        }

        sealed class Edge {
            data class ForeignKeyBetweenColumn(val foreignKey: ForeignKey) : Edge()
            data class ReferenceBetweenColumn(val reference: Reference) : Edge()
        }

        class Wrapper {
            private val builder = Graphs.graphBuilder(Graphs.directedGraph<Name, Edge>(Edge::class.java)).get()

            fun add(name: Name) {
                builder.addVertex(name)
            }

            fun add(foreignKey: ForeignKey) {
                builder.addVertex(foreignKey.sourceTable)
                builder.addVertex(foreignKey.destinationTable)

                builder.addEdge(foreignKey.sourceTable, foreignKey.destinationTable, Edge.ForeignKeyBetweenColumn(foreignKey))
            }

            fun add(reference: Reference) {
                builder.addVertex(reference.sourceTable)
                builder.addVertex(reference.destinationTable)

                builder.addEdge(reference.sourceTable, reference.destinationTable, Edge.ReferenceBetweenColumn(reference))
            }

            fun build(): DefaultDirectedGraph<Name, Edge> {
                return builder.build()
            }
        }
    }
}