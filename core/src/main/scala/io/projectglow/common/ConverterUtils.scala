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

import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

private[projectglow] object ConverterUtils {
  def arrayDataToStringList(array: ArrayData): Seq[String] = {
    array.toObjectArray(StringType).map(_.asInstanceOf[UTF8String].toString)
  }
}
