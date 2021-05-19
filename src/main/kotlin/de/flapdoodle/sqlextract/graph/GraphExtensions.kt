package de.flapdoodle.sqlextract.graph

import org.jgrapht.graph.DefaultDirectedGraph

fun <V,E> DefaultDirectedGraph<V, E>.sourcesOf(destination: V): Set<V> {
    var sources = emptySet<V>()
    val incomingEdges = incomingEdgesOf(destination)
    incomingEdges.map { edge ->
        val source = getEdgeSource(edge)
        sources = sources + source
    }
    return sources
}

fun <V,E> DefaultDirectedGraph<V, E>.targetsOf(source: V): Set<V> {
    var targets = emptySet<V>()
    val outgoingEdges = outgoingEdgesOf(source)
    outgoingEdges.map { edge ->
        val target = getEdgeTarget(edge)
        targets = targets + target
    }
    return targets
}
