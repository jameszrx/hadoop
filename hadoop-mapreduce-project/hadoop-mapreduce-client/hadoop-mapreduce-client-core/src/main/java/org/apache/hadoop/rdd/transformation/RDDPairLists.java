
package org.apache.hadoop.rdd.transformation;

import com.google.common.collect.Lists;
import org.apache.hadoop.rdd.func.TransformFunc;
import org.apache.hadoop.rdd.io.save.Saver;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.structure.RDDList;
import org.apache.hadoop.rdd.structure.RDDPairList;
import org.apache.hadoop.rdd.types.GroupedPairListType;
import org.apache.hadoop.rdd.types.RDDPairListType;
import org.apache.hadoop.rdd.types.Type;

import java.util.List;


public class RDDPairLists {

  public static <K, V> RDDList<K> keys(RDDPairList<K, V> ptable) {
    return ptable.parallel("PairLists.keys", new TransformFunc<Pair<K, V>, K>() {
      @Override
      public void process(Pair<K, V> input, Saver<K> saver) {
        saver.save(input.key());
      }
    }, ptable.getKeyType());
  }

  public static <K, V> RDDList<V> values(RDDPairList<K, V> ptable) {
    return ptable.parallel("PairLists.values", new TransformFunc<Pair<K, V>, V>() {
      @Override
      public void process(Pair<K, V> input, Saver<V> saver) {
        saver.save(input.value());
      }
    }, ptable.getValueType());
  }


  public static <K, V> Pair<K, V> getDetachedValue(RDDPairListType<K, V> tableType, Pair<K, V> value) {
    return new Pair<>(tableType.getKeyType().getDetachedValue(value.key()),
        tableType.getValueType().getDetachedValue(value.value()));
  }


  public static <K, V> Pair<K, Iterable<V>> getGroupedDetachedValue(GroupedPairListType<K, V> groupedPairListType,
      Pair<K, Iterable<V>> value) {

    RDDPairListType<K, V> tableType = groupedPairListType.getPairListType();
    List<V> detachedIterable = Lists.newArrayList();
    Type<V> valueType = tableType.getValueType();
    for (V v : value.value()) {
      detachedIterable.add(valueType.getDetachedValue(v));
    }
    return new Pair<>(tableType.getKeyType().getDetachedValue(value.key()), detachedIterable);
  }
}
