package de.flapdoodle.sqlextract.db

data class Name(
        val name: String,
        val schema: String,
) {
    fun asSQL(): String {
        return "$schema.$name"
    }

    companion object {
        fun parse(name: String): Name {
            val idx = name.indexOf('.')
            require(idx!=-1) {"invalid format: '$name', expected: SCHEMA.TABLE"}
            return Name(name = name.substring(idx + 1), schema = name.substring(0, idx))
        }
    }
}
