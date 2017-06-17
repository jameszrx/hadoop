
package org.apache.hadoop.rdd.structure;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.rdd.plan.TransformNode;
import org.apache.hadoop.rdd.types.Type;

import java.util.List;

public class UnionList<S> extends RDDListImpl<S> {

  private List<RDDListImpl<S>> dependencies;
  private long size = 0;

  UnionList(List<RDDListImpl<S>> collections) {
    super("union");
    this.dependencies = ImmutableList.copyOf(collections);
    this.stream = dependencies.get(0).getStream();
    for (RDDListImpl<S> d : dependencies) {
      size += d.getSize();
    }
  }

  @Override
  protected long size() {
    return size;
  }

  @Override
  protected void acceptPrivate(RDDListImpl.Visitor visitor) {
    visitor.visitUnionCollection(this);
  }

  @Override
  public Type<S> getType() {
    return dependencies.get(0).getType();
  }

  @Override
  public List<RDDListImpl<?>> getDependencies() {
    return ImmutableList.copyOf(dependencies);
  }

  @Override
  public TransformNode createTransformNode() {
    return null;
  }
}
