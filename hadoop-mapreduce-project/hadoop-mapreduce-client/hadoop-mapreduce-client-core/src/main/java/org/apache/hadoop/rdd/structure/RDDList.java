
package org.apache.hadoop.rdd.structure;

import org.apache.hadoop.rdd.func.FilterFunc;
import org.apache.hadoop.rdd.func.TransformFunc;
import org.apache.hadoop.rdd.io.Target;
import org.apache.hadoop.rdd.job.Stream;
import org.apache.hadoop.rdd.types.RDDPairListType;
import org.apache.hadoop.rdd.types.Type;



public interface RDDList<S> {

  Stream getStream();


  RDDList<S> union(RDDList<S>... collections);


  <T> RDDList<T> parallel(TransformFunc<S, T> transformFunc, Type<T> type);


  <T> RDDList<T> parallel(String name, TransformFunc<S, T> transformFunc, Type<T> type);


  <K, V> RDDPairList<K, V> parallel(TransformFunc<S, Pair<K, V>> transformFunc, RDDPairListType<K, V> type);


  <K, V> RDDPairList<K, V> parallel(String name, TransformFunc<S, Pair<K, V>> transformFunc, RDDPairListType<K, V> type);


  RDDList<S> write(Target target);


  Iterable<S> materialize();


  Type<S> getType();

  long getSize();


  String getName();


  RDDList<S> filter(FilterFunc<S> filterFunc);


  RDDList<S> filter(String name, FilterFunc<S> filterFunc);


  RDDList<S> sort(boolean ascending);


  RDDPairList<S, Long> count();

}
