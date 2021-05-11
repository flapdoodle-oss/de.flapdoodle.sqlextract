package de.flapdoodle.sqlextract.db

class TableGraphWalker(private val tableResolver: TableResolver) {

    fun with(tables: Set<String>): TableGraph {
        var current = TableGraph(tableResolver, listOf())
        tables.forEach {
            println("-> $it")
            current = current.inspect(it)
        }
        return current
    }

    data class TableGraph(
            private val tableResolver: TableResolver,
            private val tables: List<Table>
    ) {
        private val tableMap = tables.associateBy { it.name }

        private val foreignKeyTablesNames = tables.flatMap { table ->
            table.foreignKeys.map { fk -> fk.destinationTable }
        }.toSet()

        private val unknownTableNames = foreignKeyTablesNames - tableMap.keys

        init {
            println("tableNames: $tableMap")
            println("foreignKeyTablesNames: $foreignKeyTablesNames")
            println("unknownTableNames: $unknownTableNames")
        }

        fun unknownTableNames() = unknownTableNames

        fun add(table: Table): TableGraph {
//            require(!tableMap.contains(table.name)) { "table $table already added" }
            return if (!tableMap.contains(table.name))
                copy(tables = this.tables + table)
            else
                this
        }

        fun table(tableName: String): Table {
            val table = tableMap.get(tableName)

            require(table != null) { "could not find $tableName" }

            return table
        }

        private fun walk(): TableGraph {
            val firstUnknown = unknownTableNames().firstOrNull()
            if (firstUnknown != null) {
                println("-> $firstUnknown")
                val table = tableResolver.byName(firstUnknown)
                return add(table).walk()
            }
            return this
        }

        fun inspect(tableName: String): TableGraph {
            val table = tableResolver.byName(tableName)
            return add(table).walk()
        }
    }
}