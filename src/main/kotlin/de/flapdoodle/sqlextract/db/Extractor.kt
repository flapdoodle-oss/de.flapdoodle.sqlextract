package de.flapdoodle.sqlextract.db

import de.flapdoodle.sqlextract.cache.CachingTableRepositoryFactory
import de.flapdoodle.sqlextract.config.Extraction
import de.flapdoodle.sqlextract.config.ForeignKeys
import de.flapdoodle.sqlextract.data.DataSetCollector
import de.flapdoodle.sqlextract.data.Target
import de.flapdoodle.sqlextract.io.IO
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
        val connectionConfig = config.databaseConnection


        val connection =
            Connections.connection(connectionConfig.jdbcUrl, connectionConfig.className, connectionConfig.user, connectionConfig.password, connectionConfig.driver)

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


            val snapshot = dataSetCollector.snapshot()

            val dump = snapshot.insertSQL()
            val dumpFile = target.dumpFile()

            IO.write(dumpFile, dump.joinToString(separator = "\n\n\n"));

            val dotFile = target.dotFile()
            IO.write(dotFile, snapshot.tableGraphAsDot())
//            println("----------------")
//            dump.forEach {
//                println(it)
//                println("\n\n")
//            }
//            println("----------------")
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