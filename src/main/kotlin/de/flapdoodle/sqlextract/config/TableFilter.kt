package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.filetypes.Attributes
import java.util.function.Predicate
import java.util.regex.Pattern

data class TableFilter(
    val includes: Set<String> = emptySet(),
    val excludes: Set<String> = emptySet()
) {
    private val includesMatcher: List<Predicate<String>> = includes.map(::matchNameOrRegex)
    private val excludesMatcher: List<Predicate<String>> = excludes.map(::matchNameOrRegex)

    fun matchingTableName(name: String): Boolean {
        val ret = ((includesMatcher.isEmpty() || includesMatcher.any { it.test(name) })
                && excludesMatcher.none { it.test(name) })
//        println("$name -> $ret")
        return ret
    }

    companion object {
        private val TABLE_NAME_REGEX = Pattern.compile("^[\\p{L}_][\\p{L}\\p{N}@$#_]{0,127}$")

        internal fun isValidTableName(name: String) = TABLE_NAME_REGEX.matcher(name).matches()

        internal fun matchNameOrRegex(tableNameOrRegex: String): Predicate<String> {
            return if (isValidTableName(tableNameOrRegex)) ExactMatch(tableNameOrRegex)
            else PatternMatch(tableNameOrRegex)
        }

        data class ExactMatch(val tableName: String) : Predicate<String> {
            override fun test(p0: String) = p0 == tableName
        }

        data class PatternMatch(val tableNameRegex: String) : Predicate<String> {
            private val regex = Pattern.compile(tableNameRegex)
            override fun test(p0: String) = regex.matcher(p0).find()
        }

        fun parse(source: Attributes.Node?): TableFilter {
            if (source != null) {
                val includes = source.findValues("include", String::class)?.toSet()
                val excludes = source.findValues("exclude", String::class)?.toSet()
                return TableFilter(
                    includes = includes ?: emptySet(),
                    excludes = excludes ?: emptySet()
                )
            }
            return TableFilter()
        }
    }
}