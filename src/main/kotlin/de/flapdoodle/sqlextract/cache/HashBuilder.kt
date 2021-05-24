package de.flapdoodle.sqlextract.cache

import com.google.common.hash.Hasher
import com.google.common.hash.Hashing
import java.nio.charset.Charset

class HashBuilder(
    private val hasher: Hasher = Hashing.sha512().newHasher(),
    private val defaultCharset: Charset = Charsets.UTF_8
) {
    fun append(value: String): HashBuilder {
        hasher.putString(value, defaultCharset)
        return this
    }

    fun build(): String {
        return hasher.hash().toString()
    }

}