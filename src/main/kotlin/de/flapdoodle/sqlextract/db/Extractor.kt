package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.config.Extraction
import de.flapdoodle.sqlextract.config.ForeignKeys
import de.flapdoodle.sqlextract.data.DataSetCollector
import de.flapdoodle.sqlextract.graph.TableGraph
import de.flapdoodle.sqlextract.jdbc.Connections
import de.flapdoodle.sqlextract.jdbc.tables


class Extractor {

    fun extract(config: Extraction) {
        println("config: $config")

        val connection =
            Connections.connection(config.jdbcUrl, config.className, config.user, config.password, config.driver)

        connection.use { con ->
            val tableResolver = CachingTableResolverWrapper(
                JdbcTableResolver(
                    connection = connection,
                    postProcess = addForeignKeys(config.foreignKeys)
                )
            )

            println("All Tables")
            println("-------------------------")
            val tableNames = connection.metaData.tables().map { table ->
//            println("--> ${table.name}")
                table.name
            }
            println("-------------------------")
            println()

            val includedTables = tableNames.filter(config.tableFilter::matchingTableName)

            println("Tables")
            println("-------------------------")
            includedTables.groupBy { it.schema }.forEach { schema, list ->
                println("Schema: $schema")
                list.forEach {
                    println("-> ${it.name}")
                }
            }
//        includedTables.forEach {
//            println("-> $it")
//        }
            println("-------------------------")

            val tables = Tables.empty().add(includedTables, tableResolver)
            val tableGraph = TableGraph.of(tables.all())

            val dataSetCollector = DataSetCollector(
                connection = connection,
                tables = tables
            )

            config.dataSets.forEach { dataSet ->
                println("#############################")
                println("${dataSet.name}")
                println("#############################")
                dataSetCollector.collect(dataSet)
            }

//            config.dataSets.forEach { dataSet ->
//                println("-> ${dataSet.name}")
//
////                val dataSetTables = Tables.empty()
////                    .add(dataSet.include + dataSet.table, tableResolver)
//
//                val table = tables.get(dataSet.table)
//
//                val sqlQuery = "select * from ${dataSet.table.asSQL()} where ${dataSet.where}"
//
//                println("query: $sqlQuery")
//                con.query { prepareStatement(sqlQuery).executeQuery() }
//                    .map {
//                        table.columns.forEach { column ->
//                            val value = column(column.name, Object::class)
//                            println("${column.name}=$value")
//                        }
//                    }
////                val statement = connection.prepareStatement(sqlQuery)
////                val resultSet = statement.executeQuery()
////                while (resultSet.next()) {
////                    println("-> "+resultSet)
////                }
//            }
//            val rs: ResultSet = it.metaData.getTables(null, null, "%", null)
//            while (rs.next()) {
//                println("-> "+rs.getString(3))
//            }
            println("----------------")
            println(tableGraph.asDot())
            println("----------------")

            val dump = dataSetCollector.snapshot().insertSQL()
            println("----------------")
            println(dump)
            println("----------------")
        }
    }

    private fun addForeignKeys(foreignKeys: List<ForeignKeys>): (Table) -> Table {
        return {
            val keys = foreignKeys.filter { fk -> fk.schema == it.name.schema }
                .flatMap { fk -> fk.foreignKeys(it.name.name) }

            it.withForeignKeys(keys)
        }
    }

//    class Wrapper(wrapped: Driver) : Driver by wrapped
}