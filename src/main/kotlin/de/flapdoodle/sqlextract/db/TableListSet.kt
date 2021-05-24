package de.flapdoodle.sqlextract.db

data class TableListSet(
    private val list: List<Table> = emptyList()
) : TableSet {
    override fun all(): List<Table> {
        return list
    }

}