package de.flapdoodle.sqlextract.graph

import de.flapdoodle.sqlextract.db.Name

data class TableColumn(
    val table: Name,
    val name: String
) {
    fun asId(): String {
        return table.schema+"_"+table.name+"__"+name
    }

    fun simpleName(): String {
        return table.schema+"."+table.name+":"+name
    }
}
