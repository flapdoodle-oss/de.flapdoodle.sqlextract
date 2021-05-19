package de.flapdoodle.sqlextract.graph

import de.flapdoodle.sqlextract.db.Name

data class TableColumn(
    val table: Name,
    val name: String
)
