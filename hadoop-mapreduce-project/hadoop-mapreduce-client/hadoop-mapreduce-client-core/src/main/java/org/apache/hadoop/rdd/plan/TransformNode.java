
package org.apache.hadoop.rdd.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.rdd.func.TransformFunc;
import org.apache.hadoop.rdd.io.Source;
import org.apache.hadoop.rdd.job.Node;
import org.apache.hadoop.rdd.job.NodeTask;
import org.apache.hadoop.rdd.types.Converter;
import org.apache.hadoop.rdd.types.GroupedPairListType;
import org.apache.hadoop.rdd.types.Type;

import java.util.List;

public class TransformNode {

  private static final List<TransformNode> NO_CHILDREN = ImmutableList.of();

  private final TransformFunc func;
  private final Source<?> source;
  private final Converter outputConverter;
  private String outputName;
  private final String name;
  private final Type<?> ptype;
  private final List<TransformNode> children;


  private TransformNode(TransformFunc func, String name, Type<?> ptype, List<TransformNode> children, Converter outputConverter,
                        Source<?> source) {
    this.func = func;
    this.name = name;
    this.ptype = ptype;
    this.children = children;
    this.outputConverter = outputConverter;
    this.source = source;
  }

  private static List<TransformNode> allowsChildren() {
    return Lists.newArrayList();
  }

  public static <K, V> TransformNode createGroupingNode(String name, GroupedPairListType<K, V> ptype) {
    TransformFunc<?, ?> func = ptype.getOutputMapFunc();
    return new TransformNode(func, name, ptype, NO_CHILDREN, ptype.getGroupingConverter(), null);
  }

  public static <S> TransformNode createOutputNode(String name, Type<S> ptype) {
    Converter outputConverter = ptype.getConverter();
    TransformFunc<?, ?> func = ptype.getOutputMapFunc();
    return new TransformNode(func, name, ptype, NO_CHILDREN, outputConverter, null);
  }

  public static TransformNode createTransformNode(String name, TransformFunc<?, ?> function, Type<?> ptype) {
    return new TransformNode(function, name, ptype, allowsChildren(), null, null);
  }

  public static <S> TransformNode createInputNode(Source<S> source) {
    Type<?> ptype = source.getType();
    TransformFunc<?, ?> func = ptype.getInputMapFunc();
    return new TransformNode(func, source.toString(), ptype, allowsChildren(), null, source);
  }


  public String getName() {
    return name;
  }

  public List<TransformNode> getChildren() {
    return children;
  }

  public Source<?> getSource() {
    return source;
  }

  public Type<?> getPType() {
    return ptype;
  }

  public TransformNode addChild(TransformNode node) {
    if (!children.contains(node)) {
      this.children.add(node);
    }
    return this;
  }

  public void setOutputName(String outputName) {
    if (outputConverter == null) {
      throw new IllegalStateException("Cannot set output name w/o output converter: " + outputName);
    }
    this.outputName = outputName;
  }

  public Node toNode(boolean inputNode, Configuration conf, NodeTask nodeTask) {
    List<Node> childNodes = Lists.newArrayList();
    func.configure(conf);
    for (TransformNode child : children) {
      childNodes.add(child.toNode(false, conf, nodeTask));
    }

    Converter inputConverter = null;
    if (inputNode) {
      if (nodeTask == NodeTask.MAP) {
        inputConverter = ptype.getConverter();
      } else {
        inputConverter = ((GroupedPairListType<?, ?>) ptype).getGroupingConverter();
      }
    }
    return new Node(func, name, childNodes, inputConverter, outputConverter, outputName);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof TransformNode)) {
      return false;
    }
    if (this == other) {
      return true;
    }
    TransformNode o = (TransformNode) other;
    return (name.equals(o.name) && func.equals(o.func) && source == o.source && outputConverter == o.outputConverter);
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    return hcb.append(name).append(func).append(source).append(outputConverter).toHashCode();
  }
}
