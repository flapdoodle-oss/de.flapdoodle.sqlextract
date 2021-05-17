package de.flapdoodle.sqlextract.db

data class Name(
        val name: String,
        val catalog: String? = null,
        val schema: String? = null,
)
