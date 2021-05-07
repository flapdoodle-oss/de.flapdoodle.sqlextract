package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.ForeignKey

data class ForeignKeys(val list: List<ForeignKey>) {

    fun foreignKeys(tableName: String): List<ForeignKey> {
        return list.filter {
            it.tableName == tableName
        }
    }

    fun tables(): Set<String> {
        return list.map { it.tableName }.toSet()
    }

    companion object {
        fun parse(table: List<List<*>>?): ForeignKeys {
            val src: List<List<*>> = (table ?: emptyList())
            val mapped = src.map {
                require(it.size == 2) { "wrong format, must contain source and destination" }
                require(it[0] is String) { "can not handle first part of: $it" }
                require(it[1] is String) { "can not handle first part of: $it" }
                val source: String = it[0] as String
                val destination: String = it[1] as String
                foreignKey(source, destination)
            }

            return ForeignKeys(mapped)
        }

        private fun foreignKey(source: String, destination: String): ForeignKey {
            val s = tableAndColumn(source)
            val d = tableAndColumn(destination)
            return ForeignKey(
                    tableName = s.first,
                    columnName = s.second,
                    foreignTableName = d.first,
                    foreignColumnName = d.second
            )
        }

        private fun tableAndColumn(value: String): Pair<String, String> {
            val idx = value.indexOf('.')
            require(idx!=-1) {"wrong format: $value != <TABLE.COLUMN>"}
            return value.substring(0,idx) to value.substring(idx+1)
        }
    }
}