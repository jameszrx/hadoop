
package org.apache.hadoop.rdd.structure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.rdd.job.Stream;
import org.apache.hadoop.rdd.plan.TransformNode;
import org.apache.hadoop.rdd.types.RDDPairListType;
import org.apache.hadoop.rdd.types.Type;

import java.util.List;

public class UnionPairList<K, V> extends RDDPairListBase<K, V> {

  private RDDPairListType<K, V> type;
  private List<RDDListImpl<Pair<K, V>>> dependencies;
  private long size;


  public UnionPairList(List<RDDPairListBase<K, V>> tables) {
    super("union");
    this.type = tables.get(0).getPairListType();
    this.stream = (Stream) tables.get(0).getStream();
    this.dependencies = Lists.newArrayList();
    for (RDDPairListBase<K, V> dependent : tables) {
      this.dependencies.add(dependent);
      size += dependent.getSize();
    }
  }

  @Override
  protected long size() {
    return size;
  }

  @Override
  public RDDPairListType<K, V> getPairListType() {
    return type;
  }

  @Override
  public Type<Pair<K, V>> getType() {
    return type;
  }

  @Override
  public List<RDDListImpl<?>> getDependencies() {
    return ImmutableList.copyOf(dependencies);
  }

  @Override
  protected void acceptPrivate(RDDListImpl.Visitor visitor) {
    visitor.visitUnionCollection(new UnionList<>(dependencies));
  }

  @Override
  public TransformNode createTransformNode() {
    return null;
  }

}
