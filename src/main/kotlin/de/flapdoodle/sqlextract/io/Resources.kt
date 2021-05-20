package de.flapdoodle.sqlextract.io

import java.nio.charset.Charset

fun <T: Any,D: Any> Class<T>.readResource(path: String, map: (ByteArray) -> D): D {
    return Resources.read(this,path,map)
}

fun <T: Any> Class<T>.readResource(path: String, charset: Charset = Charsets.UTF_8): String {
    return Resources.read(this, path, { String(it, charset) })
}

object Resources {
    fun <T : Any, D : Any> read(
        javaClass: Class<T>,
        path: String,
        map: (ByteArray) -> D
    ): D {
        val stream = javaClass.getResourceAsStream(path)
        require(stream!=null) { "could not find resource $javaClass->$path"}
        val bytes = stream.readBytes()
        return map(bytes)
    }
}