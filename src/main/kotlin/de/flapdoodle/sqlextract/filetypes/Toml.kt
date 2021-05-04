package de.flapdoodle.sqlextract.filetypes

import com.moandjiezana.toml.Toml as WRAPPED

object Toml {
    fun asTree(raw: String): Attributes.Node {
        return asTree(WRAPPED().read(raw))
    }

    private fun asTree(toml: WRAPPED): Attributes.Node {
        val children = toml.entrySet().map {
            val value = it.value
            it.key to when (value) {
                is WRAPPED -> asTree(value)
                is List<*> -> asArray(value)
                is String -> asArray(listOf(value))
                is Long -> asArray(listOf(value))
                else -> throw IllegalArgumentException("not implemented: ${value.javaClass}")
            }
        }.toMap()

        return Attributes.Node(children)
    }

    private fun <T: Any> asArray(value: List<T?>): Attributes.Values<T> {
        return Attributes.Values(value)
    }
}