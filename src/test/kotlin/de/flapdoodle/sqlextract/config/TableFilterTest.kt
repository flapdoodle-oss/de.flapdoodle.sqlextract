package de.flapdoodle.sqlextract.config

import de.flapdoodle.sqlextract.db.Name
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.*

internal class TableFilterTest {

    @ParameterizedTest
    @ValueSource(strings = ["FOO","FOO_"])
    fun validTableNameSamples(tableName: String) {
        assertThat(TableFilter.isValidTableName(tableName)).isTrue
    }

    @ParameterizedTest
    @ValueSource(strings = ["^FOO","(FOO)_"])
    fun invalidTableNameSamples(tableName: String) {
        assertThat(TableFilter.isValidTableName(tableName)).isFalse
    }

    @Test
    fun exactMatchMustOnlyMatchFullTableName() {
        val testee = TableFilter.Companion.ExactMatch("FOO")
        assertThat(testee.test(table("FOO"))).isTrue
        assertThat(testee.test(table("FOOO"))).isFalse
    }

    @Test
    fun patternMatchMustFindRegexInTableName() {
        val testee = TableFilter.Companion.PatternMatch("FOO_")
        assertThat(testee.test(table("FOO_"))).isTrue
        assertThat(testee.test(table("FOO_BAR"))).isTrue
        assertThat(testee.test(table("_FOO_BAR"))).isTrue
        assertThat(testee.test(table("_FO_BAR"))).isFalse
    }

    @Test
    fun validTableNameMustGiveExactMatch() {
        val matcher = TableFilter.matchNameOrRegex("EXACT_MATCH")
        
        assertThat(matcher)
            .isEqualTo( TableFilter.Companion.And(
                    first = TableFilter.Companion.ExactMatch("EXACT_MATCH"),
                    second = TableFilter.Companion.AnySchema
            ))
    }

    @Test
    fun invalidTableNameIsHandlesAsRegex() {
        val matcher = TableFilter.matchNameOrRegex("^EXACT_MATCH")

        assertThat(matcher)
                .isEqualTo( TableFilter.Companion.And(
                        first = TableFilter.Companion.PatternMatch("^EXACT_MATCH"),
                        second = TableFilter.Companion.AnySchema
                ))
    }


    @Test
    fun matchEveryTableBecauseNothingIsExcluded() {
        val testee = TableFilter(includes = emptySet(), excludes = emptySet())
        assertThat(testee.matchingTableName("S.FOO")).isTrue
    }

    @Test
    fun matchEveryIncludedTable() {
        val testee = TableFilter(includes = setOf("FOO","BAR"), excludes = emptySet())
        assertThat(testee.matchingTableName("S.FOO")).isTrue
        assertThat(testee.matchingTableName("S.BAR")).isTrue
        assertThat(testee.matchingTableName("S.BAZ")).isFalse
    }

    @Test
    fun dontMatchTableIfExcluded() {
        val testee = TableFilter(includes = setOf("FOO","BAR"), excludes = setOf("BAR"))
        assertThat(testee.matchingTableName("S.FOO")).isTrue
        assertThat(testee.matchingTableName("S.BAR")).isFalse
        assertThat(testee.matchingTableName("S.BAZ")).isFalse
    }

    private fun table(name: String): Name {
        return Name(name = name, schema = UUID.randomUUID().toString())
    }
}