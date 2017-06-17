
package org.apache.hadoop.rdd.structure;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.rdd.func.CombineFunc;
import org.apache.hadoop.rdd.func.TransformFunc;
import org.apache.hadoop.rdd.plan.TransformNode;
import org.apache.hadoop.rdd.types.RDDPairListType;
import org.apache.hadoop.rdd.types.Type;

import java.util.List;

public class TransformPairListImpl<K, V> extends RDDPairListBase<K, V> implements RDDPairList<K, V> {

  private final RDDListImpl<?> dependency;
  private final TransformFunc<?, Pair<K, V>> func;
  private final RDDPairListType<K, V> type;

  <S> TransformPairListImpl(String name, RDDListImpl<S> dependency, TransformFunc<S, Pair<K, V>> func, RDDPairListType<K, V> ntype) {
    super(name);
    this.dependency = dependency;
    this.func = func;
    this.type = ntype;
  }

  @Override
  protected long size() {
    return dependency.getSize();
  }

  @Override
  public RDDPairListType<K, V> getPairListType() {
    return type;
  }

  @Override
  protected void acceptPrivate(RDDListImpl.Visitor visitor) {
    visitor.visitTransformPairList(this);
  }

  @Override
  public Type<Pair<K, V>> getType() {
    return type;
  }

  @Override
  public List<RDDListImpl<?>> getDependencies() {
    return ImmutableList.of(dependency);
  }

  @Override
  public TransformNode createTransformNode() {
    return TransformNode.createTransformNode(getName(), func, type);
  }

  public boolean hasCombineFunc() {
    return func instanceof CombineFunc;
  }
}
