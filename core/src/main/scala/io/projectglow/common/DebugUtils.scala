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

package io.projectglow.common

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
