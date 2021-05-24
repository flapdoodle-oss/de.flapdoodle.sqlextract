package de.flapdoodle.sqlextract.cache

import com.squareup.moshi.Moshi
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import de.flapdoodle.sqlextract.db.Table

object PersistedTables {
    private val moshi = Moshi.Builder()
//        .add(JsonTabModel.Adapter)
        //.add(BigDecimalAdapter)
        .add(KotlinJsonAdapterFactory())
        .build()

    private val modelAdapter = moshi.adapter(PersistedTablesFile::class.java)

    private data class PersistedTablesFile(
        val hash: String,
        val tables: List<Table>
    )

    fun asJson(tables: List<Table>, hash: String): String {
        val file = PersistedTablesFile(tables = tables, hash = hash)
        return modelAdapter.toJson(file)
    }

    fun fromJson(json: String, hash: String): List<Table>? {
        val file = modelAdapter.fromJson(json)
        require(file!=null) {"could not parse $json"}
        if (file.hash==hash) {
            return file.tables
        }
        return null
    }
}