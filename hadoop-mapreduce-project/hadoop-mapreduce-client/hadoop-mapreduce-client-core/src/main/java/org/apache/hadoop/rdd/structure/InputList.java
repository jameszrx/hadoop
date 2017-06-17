
package org.apache.hadoop.rdd.structure;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.rdd.io.Source;
import org.apache.hadoop.rdd.job.Stream;
import org.apache.hadoop.rdd.plan.TransformNode;
import org.apache.hadoop.rdd.types.Type;

import java.util.List;

public class InputList<S> extends RDDListImpl<S> {

  private final Source<S> source;

  public InputList(Source<S> source, Stream stream) {
    super(source.toString());
    this.source = source;
    this.stream = stream;
  }

  @Override
  public Type<S> getType() {
    return source.getType();
  }

  public Source<S> getSource() {
    return source;
  }

  @Override
  protected long size() {
    return source.getSize(stream.getConfiguration());
  }

  @Override
  protected void acceptPrivate(RDDListImpl.Visitor visitor) {
    visitor.visitInputCollection(this);
  }

  @Override
  public List<RDDListImpl<?>> getDependencies() {
    return ImmutableList.of();
  }

  @Override
  public TransformNode createTransformNode() {
    return TransformNode.createInputNode(source);
  }

  @Override
  public boolean equals(Object obj) {
    return source.equals(((InputList) obj).source);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(source).toHashCode();
  }
}
