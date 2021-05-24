package de.flapdoodle.sqlextract.db

data class TableListRepository(
    private val list: List<Table> = emptyList()
) : TableRepository {
    private val byName = list.associateBy { it.name }

    override fun all() = list

    fun find(name: Name): Table? = byName[name]

    override fun get(name: Name): Table {
        val table = find(name)
        require(table != null) { "could not find table: $name" }
        return table
    }
}
