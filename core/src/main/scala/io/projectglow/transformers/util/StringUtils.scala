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

package io.projectglow.transformers.util

object StringUtils {
  // Matches all capital letters except at beginning of string (to allow UpperCamelCase)
  private val camelCaseRe = "(?<!^)[A-Z]".r

  def toSnakeCase(s: String): String = {
    camelCaseRe.replaceAllIn(s, m => "_" + m.matched).toLowerCase
  }
}

class SnakeCaseMap[T](originalMap: Map[String, T]) extends Map[String, T] with Serializable {

  private val keySnakeCasedMap = originalMap.map(kv => kv.copy(_1 = StringUtils.toSnakeCase(kv._1)))

  override def get(k: String): Option[T] = keySnakeCasedMap.get(StringUtils.toSnakeCase(k))

  override def contains(k: String): Boolean =
    keySnakeCasedMap.contains(StringUtils.toSnakeCase(k))

  override def +[B1 >: T](kv: (String, B1)): Map[String, B1] = {
    new SnakeCaseMap(originalMap + kv)
  }

  override def iterator: Iterator[(String, T)] = keySnakeCasedMap.iterator

  override def -(key: String): Map[String, T] = {
    new SnakeCaseMap(keySnakeCasedMap.filterKeys(_ != StringUtils.toSnakeCase(key)))
  }
}
