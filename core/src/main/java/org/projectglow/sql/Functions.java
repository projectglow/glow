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

package io.projectglow.sql;

import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.unsafe.types.UTF8String;

public class Functions {
    public static GenericArrayData asciiCharSplit(UTF8String str, UTF8String split) {
         java.util.List<UTF8String> output = new java.util.ArrayList<>();
         int start = 0;
         byte c = split.getBytes()[0];
         byte[] bytes = str.getBytes();
         for (int i = 0; i < bytes.length; i++) {
           if (bytes[i] == c) {
             output.add(UTF8String.fromBytes(bytes, start, i - start));
             start = i + 1;
           }
         }

         output.add(UTF8String.fromBytes(bytes, start, bytes.length - start));

         return new GenericArrayData(output.toArray());
    }
}
