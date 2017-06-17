
package org.apache.hadoop.rdd.structure;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.rdd.func.CombineFunc;
import org.apache.hadoop.rdd.func.TransformFunc;
import org.apache.hadoop.rdd.io.save.Saver;
import org.apache.hadoop.rdd.plan.TransformNode;
import org.apache.hadoop.rdd.types.GroupedPairListType;
import org.apache.hadoop.rdd.types.Type;

import java.util.List;

//import org.apache.hadoop.rdd.structure.GroupedPairList;

public class GroupedPairList<K, V> extends RDDListImpl<Pair<K, Iterable<V>>>  {

  private final RDDPairListBase<K, V> dependency;

  private final GroupedPairListType<K, V> type;


  GroupedPairList(RDDPairListBase<K, V> dependency) {
    super("GroupByKey");
    this.dependency = dependency;
    this.type = dependency.getPairListType().getGroupedPairListType();
  }

  public void configureShuffle(Job job) {
    type.configureShuffle(job);
    job.setNumReduceTasks(1);
  }

  @Override
  protected long size() {
    return dependency.size();
  }

  @Override
  public Type<Pair<K, Iterable<V>>> getType() {
    return type;
  }

  public RDDPairList<K, V> combineValues(CombineFunc<K, V> combineFunc) {
    return new TransformPairListImpl<K, V>("combine", this, combineFunc, dependency.getPairListType());
  }

  private static class Ungroup<K, V> extends TransformFunc<Pair<K, Iterable<V>>, Pair<K, V>> {
    @Override
    public void process(Pair<K, Iterable<V>> input, Saver<Pair<K, V>> saver) {
      for (V v : input.value()) {
        saver.save(new Pair<>(input.key(), v));
      }
    }
  }

  public RDDPairList<K, V> ungroup() {
    return parallel("ungroup", new Ungroup<K, V>(), dependency.getPairListType());
  }

  @Override
  protected void acceptPrivate(RDDListImpl.Visitor visitor) {
    visitor.visitTransformGroupedPairList(this);
  }

  @Override
  public List<RDDListImpl<?>> getDependencies() {
    return ImmutableList.of(dependency);
  }

  @Override
  public TransformNode createTransformNode() {
    return TransformNode.createTransformNode(getName(), type.getInputMapFunc(), type);
  }

  public TransformNode getGroupingNode() {
    return TransformNode.createGroupingNode("", type);
  }
}
