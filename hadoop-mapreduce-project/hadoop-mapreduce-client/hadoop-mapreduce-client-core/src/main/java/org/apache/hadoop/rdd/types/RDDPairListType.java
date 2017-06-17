
package org.apache.hadoop.rdd.types;

import org.apache.hadoop.rdd.structure.Pair;


public interface RDDPairListType<K, V> extends Type<Pair<K, V>> {

  Type<K> getKeyType();


  Type<V> getValueType();


  GroupedPairListType<K, V> getGroupedPairListType();
}
