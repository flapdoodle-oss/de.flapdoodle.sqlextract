package de.flapdoodle.sqlextract.db

class CachingTableResolverWrapper(
    private val delegate: TableResolver
) : TableResolver {
    private var tableMap = emptyMap<Name, Table>()

    override fun byName(name: Name): Table {
        val cachedValue = tableMap[name]

        return if (cachedValue!=null) {
            cachedValue
        } else {
            val table = delegate.byName(name)
            tableMap = tableMap + (name to table)
            table
        }
    }
}