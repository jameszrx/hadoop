
package org.apache.hadoop.rdd.structure;

import org.apache.hadoop.rdd.io.Target;
import org.apache.hadoop.rdd.types.RDDPairListType;
import org.apache.hadoop.rdd.types.Type;

import java.util.Collection;


public interface RDDPairList<K, V> extends RDDList<Pair<K, V>> {

  RDDPairList<K, V> union(RDDPairList<K, V>... others);


  GroupedPairList<K, V> groupByKey();


//  GroupedPairList<K, V> groupByKey(int numPartitions);



  RDDPairList<K, V> write(Target target);


  RDDPairListType<K, V> getPairListType();


  Type<K> getKeyType();


  Type<V> getValueType();


  <U> RDDPairList<K, Pair<V, U>> join(RDDPairList<K, U> other);


  <U> RDDPairList<K, Pair<Collection<V>, Collection<U>>> cogroup(RDDPairList<K, U> other);


  RDDList<K> keys();


  RDDList<V> values();


}
