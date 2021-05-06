package de.flapdoodle.sqlextract.db

class TableGraphWalker(private val tableResolver: TableResolver) {

    fun startFrom(tableName: String): TableGraph {
        println("start with $tableName")
        val start = tableResolver.byName(tableName)
        val graph = TableGraph(listOf(start))
        return walk(graph)
    }

    private fun walk(graph: TableGraph): TableGraph {
        val firstUnknown = graph.unknownTableNames().firstOrNull()
        if (firstUnknown!=null) {
            println("-> $firstUnknown")
            val table = tableResolver.byName(firstUnknown)
            return walk(graph.add(table))
        }
        return graph
    }

    data class TableGraph(private val tables: List<Table>) {
        private val tableMap = tables.associateBy { it.name }

        private val foreignKeyTablesNames = tables.flatMap {
            table -> table.foreignKeys.map { fk -> fk.foreignTableName }
        }.toSet()

        private val unknownTableNames = foreignKeyTablesNames - tableMap.keys

        init {
            println("tableNames: $tableMap")
            println("foreignKeyTablesNames: $foreignKeyTablesNames")
            println("unknownTableNames: $unknownTableNames")
        }

        fun unknownTableNames() = unknownTableNames

        fun add(table: Table): TableGraph {
            require(!tableMap.contains(table.name)) {"table $table already added"}
            return copy(tables = this.tables + table)
        }

        fun table(tableName: String): Table {
            val table = tableMap.get(tableName)

            require(table!=null) {"could not find $tableName"}

            return table
        }
    }
}