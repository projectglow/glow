/*
 * Copyright 2019 The Glow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
