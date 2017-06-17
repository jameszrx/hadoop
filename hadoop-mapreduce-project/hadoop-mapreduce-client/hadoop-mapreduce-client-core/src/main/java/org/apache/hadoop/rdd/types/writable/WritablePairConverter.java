
package org.apache.hadoop.rdd.types.writable;

import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.types.Converter;

public class WritablePairConverter<K, V> implements Converter<K, V, Pair<K, V>, Pair<K, Iterable<V>>> {

  private final Class<K> keyClass;
  private final Class<V> valueClass;

  public WritablePairConverter(Class<K> keyClass, Class<V> valueClass) {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
  }

  @Override
  public Pair<K, V> convertInput(K key, V value) {
    return new Pair(key, value);
  }

  @Override
  public K outputKey(Pair<K, V> value) {
    return value.key();
  }

  @Override
  public V outputValue(Pair<K, V> value) {
    return value.value();
  }

  @Override
  public Class<K> getKeyClass() {
    return keyClass;
  }

  @Override
  public Class<V> getValueClass() {
    return valueClass;
  }

  @Override
  public Pair<K, Iterable<V>> convertIterableInput(K key, Iterable<V> value) {
    return new Pair(key, value);
  }
}
