
package org.apache.hadoop.rdd.io.seq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.types.Type;
import org.apache.hadoop.rdd.types.writable.WritableType;
import org.apache.hadoop.util.ReflectionUtils;

public class SeqFileHelper {
  static <T> Writable newInstance(Type<T> ptype, Configuration conf) {
    return (Writable) ReflectionUtils.newInstance(((WritableType) ptype).getSerializationClass(), conf);
  }

  static <T> MapFunc<Object, T> getInputMapFunc(Type<T> ptype) {
    return ptype.getInputMapFunc();
  }
}
