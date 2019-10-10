package io.projectglow.sql.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}

object KryoUtils {
  def toByteArray[T <: AnyRef](kryo: Kryo, obj: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = new Output(baos)
    kryo.writeObject(output, obj)
    output.flush()
    output.close()
    baos.toByteArray
  }

  def fromByteArray[T](kryo: Kryo, arr: Array[Byte], clazz: Class[T]): T = {
    val bais = new ByteArrayInputStream(arr)
    val input = new Input(bais)
    kryo.readObject(input, clazz)
  }
}
