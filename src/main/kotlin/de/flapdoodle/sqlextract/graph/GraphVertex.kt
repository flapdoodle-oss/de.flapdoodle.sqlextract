package de.flapdoodle.sqlextract.graph

import de.flapdoodle.sqlextract.db.Name

sealed class GraphVertex {
    abstract fun asId(): String
    abstract fun simpleName(): String

    data class Table(val table: Name): GraphVertex() {
        override fun asId(): String {
            return table.schema+"_"+table.name
        }

        override fun simpleName(): String {
            return table.schema+"."+table.name
        }
    }

    data class TableColumn(val table: Name, val name: String): GraphVertex() {
        override fun asId(): String {
            return table.schema+"_"+table.name+"__"+name
        }

        override fun simpleName(): String {
            return name
        }
    }
}
