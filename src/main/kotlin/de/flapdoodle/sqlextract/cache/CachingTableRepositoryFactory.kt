package de.flapdoodle.sqlextract.cache

import de.flapdoodle.sqlextract.config.ForeignKeys
import de.flapdoodle.sqlextract.config.PrimaryKeys
import de.flapdoodle.sqlextract.config.References
import de.flapdoodle.sqlextract.config.TableFilterList
import de.flapdoodle.sqlextract.data.Target
import de.flapdoodle.sqlextract.db.*
import de.flapdoodle.sqlextract.io.IO
import de.flapdoodle.sqlextract.types.Comparators
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.sql.Connection
import kotlin.io.path.readText

class CachingTableRepositoryFactory(
    private val fallback: TableRepositoryFactory
) : TableRepositoryFactory {
    override fun read(
        connection: Connection,
        tableFilter: TableFilterList,
        foreignKeys: List<ForeignKeys>,
        primaryKeys: List<PrimaryKeys>,
        references: List<References>,
        target: Target
    ): TableSet {
        val hash = hash(connection,tableFilter,foreignKeys,primaryKeys,references)
        val tables = readCachedTables(target.cacheFile("tables.json"), hash)
        if (tables==null) {
            val tableRepository = fallback.read(connection, tableFilter, foreignKeys, primaryKeys, references, target)
            val fallbackTables = tableRepository.all()
            writeCachedTables(fallbackTables, target.cacheFile("tables.json"), hash)
            return TableListSet(fallbackTables)
        } else {
            return TableListSet(tables)
        }
    }

    private fun readCachedTables(cacheFile: Path, hash: String): List<Table>? {
        val file = cacheFile.toFile()
        if (file.exists()) {
            require(file.isFile) {"cache file $cacheFile is not a file"}
            val json = com.google.common.io.Files.asCharSource(file,Charsets.UTF_8).read()
            return PersistedTables.fromJson(json, hash)
        }
        return null
    }

    private fun writeCachedTables(list: List<Table>, cacheFile: Path, hash: String) {
        val json = PersistedTables.asJson(list, hash)
        IO.write(cacheFile,json)
    }

    private fun hash(
        connection: Connection,
        tableFilter: TableFilterList,
        foreignKeys: List<ForeignKeys>,
        primaryKeys: List<PrimaryKeys>,
        references: List<References>
    ): String {
        val hashBuilder = HashBuilder()
            .append(connection.metaData.url)
            .append(connection.metaData.userName)
            .apply {
                append("tableFilter")
                tableFilter.filter.forEach { tableFilter ->
                    append(tableFilter.schema)
                    append("includes")
                    tableFilter.includes.sorted().forEach { append(it) }
                    append("excludes")
                    tableFilter.excludes.sorted().forEach { append(it) }
                }
                append("foreignKeys")
                foreignKeys.sortedWith(Comparators.orderingFor(ForeignKeys::schema))
                    .forEach { fk ->
                        append(fk.schema)
                        fk.list.sortedWith(ForeignKey.Comparator).forEach {
                            append(it.sourceTable.asSQL())
                            append(it.sourceColumn)
                            append(it.destinationTable.asSQL())
                            append(it.destinationColumn)
                        }
                    }
                append("primaryKeys")
                primaryKeys.sortedWith(Comparators.orderingFor(PrimaryKeys::schema))
                    .forEach { fk ->
                        append(fk.schema)
                        fk.list.sortedBy { it.first.name+":"+it.second }.forEach {
                            append(it.first.asSQL())
                            append(it.second)
                        }
                    }
                append("references")
                references.sortedWith(Comparators.orderingFor(References::schema))
                    .forEach { ref ->
                        append(ref.schema)
                        ref.list.sortedWith(Reference.Comparator).forEach {
                            append(it.sourceTable.asSQL())
                            append(it.sourceColumn)
                            append(it.destinationTable.asSQL())
                            append(it.destinationColumn)
                        }
                    }
            }

        return hashBuilder.build()
    }
}