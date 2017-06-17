
package org.apache.hadoop.rdd.structure;

import com.google.common.collect.Lists;
import org.apache.hadoop.rdd.io.Target;
import org.apache.hadoop.rdd.transformation.Cogroup;
import org.apache.hadoop.rdd.transformation.Join;
import org.apache.hadoop.rdd.transformation.RDDPairLists;
import org.apache.hadoop.rdd.types.Type;

import java.util.Collection;
import java.util.List;

public abstract class RDDPairListBase<K, V> extends RDDListImpl<Pair<K, V>> implements RDDPairList<K, V> {

  RDDPairListBase(String name) {
    super(name);
  }

  public Type<K> getKeyType() {
    return getPairListType().getKeyType();
  }

  public Type<V> getValueType() {
    return getPairListType().getValueType();
  }

  public GroupedPairList<K, V> groupByKey() {
    return new GroupedPairList<>(this);
  }

  @Override
  public RDDPairList<K, V> union(RDDPairList<K, V>... others) {
    List<RDDPairListBase<K, V>> internal = Lists.newArrayList();
    internal.add(this);
    for (RDDPairList<K, V> table : others) {
      internal.add((RDDPairListBase<K, V>) table);
    }
    return new UnionPairList<>(internal);
  }

  @Override
  public RDDPairList<K, V> write(Target target) {
    getStream().write(this, target);
    return this;
  }

  @Override
  public <U> RDDPairList<K, Pair<V, U>> join(RDDPairList<K, U> other) {
    return Join.join(this, other);
  }

  @Override
  public <U> RDDPairList<K, Pair<Collection<V>, Collection<U>>> cogroup(RDDPairList<K, U> other) {
    return Cogroup.cogroup(this, other);
  }

  @Override
  public RDDList<K> keys() {
    return RDDPairLists.keys(this);
  }

  @Override
  public RDDList<V> values() {
    return RDDPairLists.values(this);
  }


}
