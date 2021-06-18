package de.flapdoodle.sqlextract.data

import de.flapdoodle.sqlextract.db.Table

data class RowKey(val table: Table, val key: Map<String, Any?>) {
    init {
        require(key.isNotEmpty()) {"key is empty"}
    }
}