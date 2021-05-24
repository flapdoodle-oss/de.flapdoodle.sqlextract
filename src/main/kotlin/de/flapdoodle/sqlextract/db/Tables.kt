package de.flapdoodle.sqlextract.db

@Deprecated("see TableListRepository")
data class Tables internal constructor(
    private val list: List<Table> = emptyList()
): TableSet {
    private val byName = list.associateBy { it.name }

    override fun all() = list

    fun find(name: Name): Table? = byName[name]

    fun get(name: Name): Table {
        val table = find(name)
        require(table != null) { "could not find table: $name" }
        return table
    }

    @Deprecated("use add(names...)")
    fun add(name: Name, resolver: TableResolver): Tables {
        return if (!byName.containsKey(name)) {
            add(resolver.byName(name))
                .resolveMissingTables(resolver)
        } else {
            this
        }
    }

    fun add(names: Iterable<Name>, resolver: TableResolver): Tables {
        val newTables = names.filter { !byName.containsKey(it) }
            .map(resolver::byName)

        return if (newTables.isNotEmpty())
            copy(list = list + newTables).resolveMissingTables(resolver)
        else
            this
    }

    private fun add(table: Table): Tables {
        require(!byName.containsKey(table.name)) { "table $table already exist" }
        return copy(list = list + table)
    }

    internal fun missingTableDefinitions(): List<Name> {
        val foreignKeyDestinationTables = list.flatMap { it.destinationTables() }
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
        fun empty() = Tables()
    }
}
