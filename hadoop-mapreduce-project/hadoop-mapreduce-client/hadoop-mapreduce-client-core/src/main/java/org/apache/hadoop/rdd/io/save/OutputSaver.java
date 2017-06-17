
package org.apache.hadoop.rdd.io.save;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.rdd.types.Converter;

public class OutputSaver<T, K, V> implements Saver<T> {

  private final Converter<K, V, Object, Object> converter;
  private final TaskInputOutputContext<?, ?, K, V> context;

  public OutputSaver(Converter<K, V, Object, Object> converter, TaskInputOutputContext<?, ?, K, V> context) {
    this.converter = converter;
    this.context = context;
  }

  public void save(T saved) {
    try {
      K key = converter.outputKey(saved);
      V value = converter.outputValue(saved);
      this.context.write(key, value);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


}
