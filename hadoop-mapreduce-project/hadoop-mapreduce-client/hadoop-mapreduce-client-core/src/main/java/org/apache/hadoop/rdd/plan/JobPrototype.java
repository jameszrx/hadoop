
package org.apache.hadoop.rdd.plan;

import com.google.common.collect.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.rdd.io.Cache;
import org.apache.hadoop.rdd.io.Target;
import org.apache.hadoop.rdd.job.*;
import org.apache.hadoop.rdd.structure.GroupedPairList;
import org.apache.hadoop.rdd.structure.RDDListImpl;
import org.apache.hadoop.rdd.structure.TransformPairListImpl;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JobPrototype {

  public static JobPrototype createMapReduceJob(GroupedPairList<?, ?> group, Set<NodePath> inputs, Path workingPath) {
    return new JobPrototype(inputs, group, workingPath);
  }

  public static JobPrototype createMapOnlyJob(HashMultimap<Target, NodePath> mapNodePaths, Path workingPath) {
    return new JobPrototype(mapNodePaths, workingPath);
  }

  private final Set<NodePath> mapNodePaths;
  private final GroupedPairList<?, ?> group;
  private final Set<JobPrototype> dependencies = Sets.newHashSet();
  private final Map<RDDListImpl<?>, TransformNode> nodes = Maps.newHashMap();
  private final Path workingPath;

  private HashMultimap<Target, NodePath> targetsToNodePaths;
  private TransformPairListImpl<?, ?> combineFuncPairList;

  private RDDJob job;

  private JobPrototype(Set<NodePath> inputs, GroupedPairList<?, ?> group, Path workingPath) {
    this.mapNodePaths = ImmutableSet.copyOf(inputs);
    this.group = group;
    this.workingPath = workingPath;
    this.targetsToNodePaths = null;
  }

  private JobPrototype(HashMultimap<Target, NodePath> outputPaths, Path workingPath) {
    this.group = null;
    this.mapNodePaths = null;
    this.workingPath = workingPath;
    this.targetsToNodePaths = outputPaths;
  }

  public void addReducePaths(HashMultimap<Target, NodePath> outputPaths) {
    this.targetsToNodePaths = outputPaths;
  }

  public void addDependency(JobPrototype dependency) {
    this.dependencies.add(dependency);
  }

  public RDDJob getRDDJob(Class<?> jarClass, Configuration conf, Stream stream) throws IOException {
    if (job == null) {
      job = build(jarClass, conf, stream);
      for (JobPrototype proto : dependencies) {
        job.addDependingJob(proto.getRDDJob(jarClass, conf, stream));
      }
    }
    return job;
  }

  private RDDJob build(Class<?> jarClass, Configuration conf, Stream stream) throws IOException {
    Job job = new Job(conf);
    conf = job.getConfiguration();
    conf.set("RDDWorkPath", workingPath.toString());
    job.setJarByClass(jarClass);

    Set<TransformNode> outputNodes = Sets.newHashSet();
    Set<Target> targets = targetsToNodePaths.keySet();
    Path outputPath = new Path(workingPath, "output");
    OutputHandler outputHandler = new OutputHandler(job, outputPath, group == null);
    for (Target target : targets) {
      TransformNode node = null;
      for (NodePath nodePath : targetsToNodePaths.get(target)) {
        if (node == null) {
          RDDListImpl<?> collect = nodePath.tail();
          node = TransformNode.createOutputNode(target.toString(), collect.getType());
          outputHandler.configureNode(node, target);
        }
        outputNodes.add(walkPath(nodePath.descendingIterator(), node));
      }
    }

    job.setMapperClass(RDDMapper.class);
    List<TransformNode> inputNodes;
    TransformNode reduceNode = null;
    if (group != null) {
      job.setReducerClass(RDDReducer.class);
      List<TransformNode> reduceNodes = Lists.newArrayList(outputNodes);
      serialize(reduceNodes, conf, workingPath, NodeTask.REDUCE);
      reduceNode = reduceNodes.get(0);

      if (combineFuncPairList != null) {
        job.setCombinerClass(RDDCombiner.class);
        TransformNode combinerInputNode = group.createTransformNode();
        TransformNode combineNode = combineFuncPairList.createTransformNode();
        combineNode.addChild(group.getGroupingNode());
        combinerInputNode.addChild(combineNode);
        serialize(ImmutableList.of(combinerInputNode), conf, workingPath, NodeTask.COMBINE);
      }

      group.configureShuffle(job);

      TransformNode mapOutputNode = group.getGroupingNode();
      Set<TransformNode> mapNodes = Sets.newHashSet();
      for (NodePath nodePath : mapNodePaths) {
        Iterator<RDDListImpl<?>> iter = nodePath.descendingIterator();
        iter.next();
        mapNodes.add(walkPath(iter, mapOutputNode));
      }
      inputNodes = Lists.newArrayList(mapNodes);
    } else {
      job.setNumReduceTasks(0);
      inputNodes = Lists.newArrayList(outputNodes);
    }
    serialize(inputNodes, conf, workingPath, NodeTask.MAP);

    TransformNode inputNode = inputNodes.get(0);
    inputNode.getSource().configureSource(job);

    job.setJobName(createJobName(stream.getName(), inputNodes, reduceNode));

    return new RDDJob(job, outputPath, outputHandler);
  }

  private void serialize(List<TransformNode> tNodes, Configuration conf, Path workingPath, NodeTask context)
      throws IOException {
    List<Node> nodes = Lists.newArrayList();
    for (TransformNode node : tNodes) {
      nodes.add(node.toNode(true, conf, context));
    }
    Path path = new Path(workingPath, context.toString());
    Cache.write(conf, path, nodes);
  }

  private String createJobName(String streamName, List<TransformNode> mapNodes, TransformNode reduceNode) {
    JobNameBuilder builder = new JobNameBuilder(streamName);
    builder.visit(mapNodes);
    if (reduceNode != null) {
      builder.visit(reduceNode);
    }
    return builder.build();
  }

  private TransformNode walkPath(Iterator<RDDListImpl<?>> iter, TransformNode working) {
    while (iter.hasNext()) {
      RDDListImpl<?> collect = iter.next();
      if (combineFuncPairList != null && !(collect instanceof GroupedPairList)) {
        combineFuncPairList = null;
      } else if (collect instanceof TransformPairListImpl && ((TransformPairListImpl<?, ?>) collect).hasCombineFunc()) {
        combineFuncPairList = (TransformPairListImpl<?, ?>) collect;
      }
      if (!nodes.containsKey(collect)) {
        nodes.put(collect, collect.createTransformNode());
      }
      TransformNode dependent = nodes.get(collect);
      dependent.addChild(working);
      working = dependent;
    }
    return working;
  }
}
