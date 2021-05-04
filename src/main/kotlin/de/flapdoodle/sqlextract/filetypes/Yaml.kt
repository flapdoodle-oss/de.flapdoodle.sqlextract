package de.flapdoodle.sqlextract.filetypes

import java.util.*
import org.yaml.snakeyaml.Yaml as WRAPPED

object Yaml {
    fun asTree(raw: String): Attributes.Node {
        val wrapped = WRAPPED().load<Map<String,Any>>(raw) ?: emptyMap()
        return asTree(wrapped)
    }

    private fun asTree(yaml: Map<String, Any>): Attributes.Node {
        val children = yaml.map {
            val value: Any = it.value
            it.key to when (value) {
                is Map<*,*> -> asTree(value as Map<String, Any>)
                is List<*> -> asArray(value)
                is String, is Int, is Date -> asArray(listOf(value))
                else -> throw IllegalArgumentException("not implemented: ${value.javaClass}")
            }
        }.toMap()

        return Attributes.Node(children)
    }

    private fun <T> asArray(value: List<T>): Attributes.Values<T> {
        return Attributes.Values(value)
    }

}