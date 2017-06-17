
package org.apache.hadoop.rdd.transformation;

import com.google.common.collect.Lists;
import org.apache.hadoop.rdd.func.MapValuesFunc;
import org.apache.hadoop.rdd.func.TransformFunc;
import org.apache.hadoop.rdd.io.save.Saver;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.structure.RDDPairList;
import org.apache.hadoop.rdd.types.Type;
import org.apache.hadoop.rdd.types.writable.Writables;

import java.util.Collection;

public class Cogroup {


  public static <K, U, V> RDDPairList<K, Pair<Collection<U>, Collection<V>>> cogroup(RDDPairList<K, U> left, RDDPairList<K, V> right) {
    Type<K> keyType = left.getPairListType().getKeyType();
    Type<U> leftType = left.getPairListType().getValueType();
    Type<V> rightType = right.getPairListType().getValueType();
    Type<Pair<U, V>> itype = Writables.pairs(leftType, rightType);

    RDDPairList<K, Pair<U, V>> cgLeft = left.parallel("coGroupTag1", new CogroupFunc1<K, U, V>(),
            Writables.tableOf(keyType, itype));
    RDDPairList<K, Pair<U, V>> cgRight = right.parallel("coGroupTag2", new CogroupFunc2<K, U, V>(),
            Writables.tableOf(keyType, itype));

    RDDPairList<K, Pair<U, V>> both = cgLeft.union(cgRight);

    Type<Pair<Collection<U>, Collection<V>>> otype = Writables.pairs(Writables.collections(leftType), Writables.collections(rightType));
    return both.groupByKey().parallel("cogroup", new PostGroupFunc<K, U, V>(), Writables.tableOf(keyType, otype));
  }

  private static class CogroupFunc1<K, V, U> extends MapValuesFunc<K, V, Pair<V, U>> {
    @Override
    public Pair<V, U> map(V v) {
      return new Pair<>(v, null);
    }
  }

  private static class CogroupFunc2<K, V, U> extends MapValuesFunc<K, U, Pair<V, U>> {
    @Override
    public Pair<V, U> map(U u) {
      return new Pair<>(null, u);
    }
  }

  private static class PostGroupFunc<K, V, U> extends
          TransformFunc<Pair<K, Iterable<Pair<V, U>>>, Pair<K, Pair<Collection<V>, Collection<U>>>> {
    @Override
    public void process(Pair<K, Iterable<Pair<V, U>>> input,
        Saver<Pair<K, Pair<Collection<V>, Collection<U>>>> saver) {
      Collection<V> cv = Lists.newArrayList();
      Collection<U> cu = Lists.newArrayList();
      for (Pair<V, U> pair : input.value()) {
        if (pair.key() != null) {
          cv.add(pair.key());
        } else if (pair.value() != null) {
          cu.add(pair.value());
        }
      }
      saver.save(new Pair<>(input.key(), new Pair<>(cv, cu)));
    }
  }

}
