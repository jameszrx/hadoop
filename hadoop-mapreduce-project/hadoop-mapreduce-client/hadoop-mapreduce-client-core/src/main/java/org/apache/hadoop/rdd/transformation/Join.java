
package org.apache.hadoop.rdd.transformation;

import org.apache.hadoop.rdd.func.JoinFunc;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.structure.GroupedPairList;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.structure.RDDPairList;
import org.apache.hadoop.rdd.types.RDDPairListType;
import org.apache.hadoop.rdd.types.writable.Writables;



public class Join {

  public static <K, U, V> RDDPairList<K, Pair<U, V>> join(RDDPairList<K, U> left, RDDPairList<K, V> right) {
    return join(left, right, new JoinFunc<>(left.getKeyType(), left.getValueType()));
  }

  public static <K, U, V> RDDPairList<K, Pair<U, V>> join(RDDPairList<K, U> left, RDDPairList<K, V> right, JoinFunc<K, U, V> joinFunc) {

    GroupedPairList<Pair<K, Integer>, Pair<U, V>> grouped = preJoin(left, right);
    RDDPairListType<K, Pair<U, V>> ret = Writables
        .tableOf(left.getKeyType(), Writables.pairs(left.getValueType(), right.getValueType()));
    return grouped.parallel("join" + grouped.getName(), joinFunc, ret);
  }

  private static <K, U, V> GroupedPairList<Pair<K, Integer>, Pair<U, V>> preJoin(RDDPairList<K, U> left, RDDPairList<K, V> right) {

    RDDPairListType<Pair<K, Integer>, Pair<U, V>> ptt = Writables.tableOf(Writables.pairs(left.getKeyType(), Writables.ints()),
            Writables.pairs(left.getValueType(), right.getValueType()));

    RDDPairList<Pair<K, Integer>, Pair<U, V>> tag1 = left.parallel("joinTagLeft",
        new MapFunc<Pair<K, U>, Pair<Pair<K, Integer>, Pair<U, V>>>() {
          @Override
          public Pair<Pair<K, Integer>, Pair<U, V>> map(Pair<K, U> input) {
            return new Pair<>(new Pair<>(input.key(), 0), new Pair<>(input.value(), (V) null));
          }
        }, ptt);
    RDDPairList<Pair<K, Integer>, Pair<U, V>> tag2 = right.parallel("joinTagRight",
        new MapFunc<Pair<K, V>, Pair<Pair<K, Integer>, Pair<U, V>>>() {
          @Override
          public Pair<Pair<K, Integer>, Pair<U, V>> map(Pair<K, V> input) {
            return new Pair<>(new Pair<>(input.key(), 1), new Pair<>((U) null, input.value()));
          }
        }, ptt);
    return (tag1.union(tag2)).groupByKey();
  }
}
