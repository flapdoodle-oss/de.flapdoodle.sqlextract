package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.cache.CachingTableRepositoryFactory
import de.flapdoodle.sqlextract.config.Extraction
import de.flapdoodle.sqlextract.config.ForeignKeys
import de.flapdoodle.sqlextract.data.DataSetCollector
import de.flapdoodle.sqlextract.data.Target
import de.flapdoodle.sqlextract.jdbc.Connections
import java.nio.file.Path


class Extractor(
    private val tableRepositoryFactory: TableRepositoryFactory = CachingTableRepositoryFactory(
        fallback = TableFromForeignKeyPathRepositoryFactory()
    )
) {

    fun extract(config: Extraction, targetPath: Path) {
        println("config: $config")
        val target = Target(targetPath)


        val connection =
            Connections.connection(config.jdbcUrl, config.className, config.user, config.password, config.driver)

        connection.use { con ->
            val tables = tableRepositoryFactory.read(connection, config.tableFilter, config.foreignKeys, target)

//            val tableGraph = TableGraph.of(tables.all())

            val dataSetCollector = DataSetCollector(
                connection = connection,
                tableSet = tables
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
//            println("----------------")
//            println(tableGraph.asDot())
//            println("----------------")

            val dump = dataSetCollector.snapshot().insertSQL()
            println("----------------")
            dump.forEach {
                println(it)
                println("\n\n")
            }
            println("----------------")
        }
    }

    private fun addForeignKeys(foreignKeys: List<ForeignKeys>): (Table) -> Table {
        return {
            val keys = foreignKeys.filter { fk -> fk.schema == it.name.schema }
                .flatMap { fk -> fk.foreignKeys(it.name) }

            it.withForeignKeys(keys)
        }
    }

//    class Wrapper(wrapped: Driver) : Driver by wrapped
}