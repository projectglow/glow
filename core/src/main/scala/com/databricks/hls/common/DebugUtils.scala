/**
 * Copyright (C) 2018 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.hls.common

import java.lang.reflect.Modifier

object DebugUtils {

  /**
   * Dump the fields and 0-arg method values from a given object. Should only be used during
   * development since some method invocations could have side effects.
   */
  def dumpObject(o: Object, exclusions: Set[String]): String = {
    val methods = o
      .getClass
      .getDeclaredMethods
      .filter(
        m =>
          Modifier.isPublic(m.getModifiers) && m.getParameterCount == 0 &&
          !exclusions.contains(m.getName)
      )
      .map(m => s"\t${m.getName}: ${m.invoke(o)}")
    val fields = o
      .getClass
      .getFields
      .filter(f => Modifier.isPublic(f.getModifiers) && !exclusions.contains(f.getName))
      .map(f => s"\t${f.getName}: ${f.get(o)}")
    val allProps = methods ++ fields
    s"${o.getClass.getSimpleName}\n${allProps.mkString("\n")}"
  }
}
