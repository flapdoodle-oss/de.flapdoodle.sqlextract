package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.config.Extraction
import de.flapdoodle.sqlextract.config.ForeignKeys
import de.flapdoodle.sqlextract.jdbc.Connections
import de.flapdoodle.sqlextract.jdbc.query
import java.net.URL
import java.net.URLClassLoader
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager


class Extractor {

    fun extract(config: Extraction) {
        println("config: $config")

        val connection = Connections.connection(config.jdbcUrl, config.className, config.user,config.password,config.driver)

        val tableResolver = JdbcTableResolver(
                connection = connection,
                postProcess = addForeignKeys(config.foreignKeys)
        )
        val tableGraph = TableGraphWalker(tableResolver)
                .with(config.foreignKeys.tables())

        connection.use { con ->
            config.dataSets.forEach { dataSet ->
                println("-> ${dataSet.name}")

                val graph = tableGraph.inspect(dataSet.table)

                println("--> $graph")

                val sqlQuery = "select * from ${dataSet.table} where ${dataSet.where}"
                val table = graph.table(dataSet.table)

                println("query: $sqlQuery")
                con.query { prepareStatement(sqlQuery).executeQuery() }
                        .map {
                            table.columns.forEach { column ->
                                val value = column(column.name, Object::class)
                                println("${column.name}=$value")
                            }
                        }
//                val statement = connection.prepareStatement(sqlQuery)
//                val resultSet = statement.executeQuery()
//                while (resultSet.next()) {
//                    println("-> "+resultSet)
//                }
            }
//            val rs: ResultSet = it.metaData.getTables(null, null, "%", null)
//            while (rs.next()) {
//                println("-> "+rs.getString(3))
//            }
        }
    }

    private fun addForeignKeys(foreignKeys: ForeignKeys): (Table) -> Table {
        return {
            val keys = foreignKeys.foreignKeys(it.name)
            it.withForeignKeys(keys)
        }
    }

//    class Wrapper(wrapped: Driver) : Driver by wrapped
}