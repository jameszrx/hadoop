
package org.apache.hadoop.rdd.structure;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.rdd.func.TransformFunc;
import org.apache.hadoop.rdd.plan.TransformNode;
import org.apache.hadoop.rdd.types.Type;

import java.util.List;

public class TransformListImpl<S> extends RDDListImpl<S> {

  private final TransformFunc<Object, S> func;
  private final RDDListImpl<Object> dependency;
  private final Type<S> ntype;

  <T> TransformListImpl(String name, RDDListImpl<T> dependency, TransformFunc<T, S> func, Type<S> ntype) {
    super(name);
    this.dependency = (RDDListImpl<Object>) dependency;
    this.func = (TransformFunc<Object, S>) func;
    this.ntype = ntype;
  }

  @Override
  protected long size() {
    return dependency.getSize();
  }

  @Override
  public Type<S> getType() {
    return ntype;
  }

  @Override
  protected void acceptPrivate(RDDListImpl.Visitor visitor) {
    visitor.visitTransformList(this);
  }

  @Override
  public List<RDDListImpl<?>> getDependencies() {
    return ImmutableList.of(dependency);
  }

  @Override
  public TransformNode createTransformNode() {
    return TransformNode.createTransformNode(getName(), func, ntype);
  }
}
