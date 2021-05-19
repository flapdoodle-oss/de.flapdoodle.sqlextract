package de.flapdoodle.sqlextract.graph

import de.flapdoodle.sqlextract.db.Name

sealed class GraphVertex(open val table: Name) {
    abstract fun asId(): String
    abstract fun simpleName(): String

    data class Table(override val table: Name): GraphVertex(table) {
        override fun asId(): String {
            return table.schema+"_"+table.name
        }

        override fun simpleName(): String {
            return table.schema+"."+table.name
        }
    }

    data class TableColumn(override val table: Name, val name: String): GraphVertex(table) {
        override fun asId(): String {
            return table.schema+"_"+table.name+"__"+name
        }

        override fun simpleName(): String {
            return name
        }
    }
}
