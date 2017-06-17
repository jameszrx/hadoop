
package org.apache.hadoop.rdd.types;

import java.io.Serializable;


public interface Converter<K, V, S, T> extends Serializable {
  S convertInput(K key, V value);

  T convertIterableInput(K key, Iterable<V> value);

  K outputKey(S value);

  V outputValue(S value);

  Class<K> getKeyClass();

  Class<V> getValueClass();
}
