package de.flapdoodle.sqlextract.db

data class Tables internal constructor(
    private val list: List<Table> = emptyList()
) {
    private val byName = list.associateBy { it.name }

    fun find(name: String): Table? = byName[name]

    fun get(name: String): Table {
        val table = find(name)
        require(table!=null) {"could not find table: $name"}
        return table
    }

    fun add(name: String, resolver: TableResolver): Tables {
        return if (!byName.containsKey(name)) {
            add(resolver.byName(name))
                .resolveMissingTables(resolver)
        } else {
            this
        }
    }

    private fun add(table: Table): Tables {
        require(!byName.containsKey(table.name)) { "table $table already exist" }
        return copy(list = list + table)
    }

    internal fun missingTableDefinitions(): List<String> {
        val foreignKeyDestinationTables = list.flatMap { it.foreignKeys }.map { it.destinationTable }
        return foreignKeyDestinationTables - byName.keys
    }

    private fun resolveMissingTables(resolver: TableResolver): Tables {
        var current = this
        do {
            val firstMissing = current.missingTableDefinitions().singleOrNull();
            if (firstMissing != null) {
                val table = resolver.byName(firstMissing)
                current = current.add(table)
            }
        } while (firstMissing != null)

        return current
    }


    companion object {
        fun tables(names: Iterable<String>, resolver: TableResolver): Tables {
            return Tables(names.map(resolver::byName)).resolveMissingTables(resolver)
        }
    }
}
