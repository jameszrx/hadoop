
package org.apache.hadoop.rdd.types.writable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.rdd.types.Converter;

class WritableValueConverter<W> implements Converter<Object, W, W, Iterable<W>> {

  private final Class<W> serializationClass;

  public WritableValueConverter(Class<W> serializationClass) {
    this.serializationClass = serializationClass;
  }

  @Override
  public W convertInput(Object key, W value) {
    return value;
  }

  @Override
  public Object outputKey(W value) {
    return NullWritable.get();
  }

  @Override
  public W outputValue(W value) {
    return value;
  }

  @Override
  public Class<Object> getKeyClass() {
    return (Class<Object>) (Class<?>) NullWritable.class;
  }

  @Override
  public Class<W> getValueClass() {
    return serializationClass;
  }

  @Override
  public Iterable<W> convertIterableInput(Object key, Iterable<W> value) {
    return value;
  }
}