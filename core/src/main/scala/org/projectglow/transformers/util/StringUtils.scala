package org.projectglow.transformers.util

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
