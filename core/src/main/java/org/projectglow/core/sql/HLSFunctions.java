package org.projectglow.core.sql;

import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.unsafe.types.UTF8String;

public class HLSFunctions {
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
