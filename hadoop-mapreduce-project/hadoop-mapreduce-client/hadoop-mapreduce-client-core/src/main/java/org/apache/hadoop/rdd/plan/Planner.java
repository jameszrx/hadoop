
package org.apache.hadoop.rdd.plan;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.rdd.io.SourceTarget;
import org.apache.hadoop.rdd.io.Target;
import org.apache.hadoop.rdd.job.Executor;
import org.apache.hadoop.rdd.job.Stream;
import org.apache.hadoop.rdd.structure.*;

import java.io.IOException;
import java.util.*;

public class Planner {

  private static final Comparator<RDDListImpl<?>> comparator = (left, right) -> right.getDepth() - left.getDepth();

  private final Stream stream;
  private final Map<RDDListImpl<?>, Set<Target>> outputs;

  public Planner(Stream stream, Map<RDDListImpl<?>, Set<Target>> outputs) {
    this.stream = stream;
    this.outputs = new TreeMap<>(comparator);
    this.outputs.putAll(outputs);
  }

  public Executor plan(Class<?> jarClass, Configuration conf) throws IOException {

    NodeVisitor visitor = new NodeVisitor();
    for (RDDListImpl<?> output : outputs.keySet()) {
      visitor.visitOutput(output);
    }

    Map<RDDListImpl<?>, Set<NodePath>> nodePaths = visitor.getNodePaths();
    Map<RDDListImpl<?>, JobPrototype> assignments = Maps.newHashMap();
    Map<RDDListImpl<?>, Set<JobPrototype>> jobDependencies = new HashMap<RDDListImpl<?>, Set<JobPrototype>>();


    Set<GroupedPairList<?, ?>> workingGroupings;
    while (!(workingGroupings = getWorkingGroupings(nodePaths)).isEmpty()) {

      for (GroupedPairList<?, ?> grouping : workingGroupings) {
        Set<NodePath> mapInputPaths = nodePaths.get(grouping);
        JobPrototype proto = JobPrototype.createMapReduceJob(grouping, mapInputPaths, stream.createTempPath());
        assignments.put(grouping, proto);
        if (jobDependencies.containsKey(grouping)) {
          for (JobPrototype dependency : jobDependencies.get(grouping)) {
            proto.addDependency(dependency);
          }
        }
      }

      Map<GroupedPairList<?, ?>, Set<NodePath>> dependencyPaths = getDependencyPaths(workingGroupings, nodePaths);
      for (Map.Entry<GroupedPairList<?, ?>, Set<NodePath>> entry : dependencyPaths.entrySet()) {
        GroupedPairList<?, ?> grouping = entry.getKey();
        Set<NodePath> currentNodePaths = entry.getValue();

        JobPrototype proto = assignments.get(grouping);
        Set<NodePath> gbkPaths = Sets.newHashSet();
        for (NodePath nodePath : currentNodePaths) {
          RDDListImpl<?> tail = nodePath.tail();
          if (tail instanceof GroupedPairList) {
            gbkPaths.add(nodePath);
            if (!jobDependencies.containsKey(tail)) {
              jobDependencies.put(tail, Sets.newHashSet());
            }
            jobDependencies.get(tail).add(proto);
          }
        }

        if (!gbkPaths.isEmpty()) {
          handleGroupingDependencies(gbkPaths, currentNodePaths);
        }


        HashMultimap<Target, NodePath> reduceOutputs = HashMultimap.create();
        for (NodePath nodePath : currentNodePaths) {
          assignments.put(nodePath.tail(), proto);
          for (Target target : outputs.get(nodePath.tail())) {
            reduceOutputs.put(target, nodePath);
          }
        }
        proto.addReducePaths(reduceOutputs);
        nodePaths.remove(grouping);
      }
    }


    if (!nodePaths.isEmpty()) {
      for (Map.Entry<RDDListImpl<?>, Set<NodePath>> entry : nodePaths.entrySet()) {
        RDDListImpl<?> collect = entry.getKey();
        if (!assignments.containsKey(collect)) {
          HashMultimap<Target, NodePath> mapOutputs = HashMultimap.create();
          for (NodePath nodePath : entry.getValue()) {
            for (Target target : outputs.get(nodePath.tail())) {
              mapOutputs.put(target, nodePath);
            }
          }
          JobPrototype proto = JobPrototype.createMapOnlyJob(mapOutputs, stream.createTempPath());
          if (jobDependencies.containsKey(collect)) {
            for (JobPrototype dependency : jobDependencies.get(collect)) {
              proto.addDependency(dependency);
            }
          }
          assignments.put(collect, proto);
        }
      }
    }

    Executor exec = new Executor(jarClass);
    for (JobPrototype proto : Sets.newHashSet(assignments.values())) {
      exec.addJob(proto.getRDDJob(jarClass, conf, stream));
    }
    return exec;
  }

  private Map<GroupedPairList<?, ?>, Set<NodePath>> getDependencyPaths(Set<GroupedPairList<?, ?>> workingGroupings,
                                                                       Map<RDDListImpl<?>, Set<NodePath>> nodePaths) {
    Map<GroupedPairList<?, ?>, Set<NodePath>> dependencyPaths = Maps.newHashMap();
    for (GroupedPairList<?, ?> grouping : workingGroupings) {
      dependencyPaths.put(grouping, Sets.newHashSet());
    }

    for (RDDListImpl<?> target : nodePaths.keySet()) {
      if (!workingGroupings.contains(target)) {
        for (NodePath nodePath : nodePaths.get(target)) {
          if (workingGroupings.contains(nodePath.head())) {
            dependencyPaths.get(nodePath.head()).add(nodePath);
          }
        }
      }
    }
    return dependencyPaths;
  }

  private int getSplitIndex(Set<NodePath> currentNodePaths) {
    List<Iterator<RDDListImpl<?>>> iters = Lists.newArrayList();
    for (NodePath nodePath : currentNodePaths) {
      Iterator<RDDListImpl<?>> iter = nodePath.iterator();
      iter.next();
      iters.add(iter);
    }
    boolean end = false;
    int splitIndex = -1;
    while (!end) {
      splitIndex++;
      RDDListImpl<?> current = null;
      for (Iterator<RDDListImpl<?>> iter : iters) {
        if (iter.hasNext()) {
          RDDListImpl<?> next = iter.next();
          if (next instanceof GroupedPairList) {
            end = true;
            break;
          } else if (current == null) {
            current = next;
          } else if (current != next) {
            end = true;
            break;
          }
        } else {
          end = true;
          break;
        }
      }
    }

    return splitIndex;
  }

  private void handleGroupingDependencies(Set<NodePath> gbkPaths, Set<NodePath> currentNodePaths) throws IOException {
    int splitIndex = getSplitIndex(currentNodePaths);
    RDDListImpl<?> splitTarget = currentNodePaths.iterator().next().get(splitIndex);
    if (!outputs.containsKey(splitTarget)) {
      outputs.put(splitTarget, Sets.newHashSet());
    }

    SourceTarget srcTarget = null;
    Target targetToReplace = null;
    for (Target t : outputs.get(splitTarget)) {
      if (t instanceof SourceTarget) {
        srcTarget = (SourceTarget<?>) t;
        break;
      } else {
        srcTarget = t.asSourceTarget(splitTarget.getType());
        if (srcTarget != null) {
          targetToReplace = t;
          break;
        }
      }
    }
    if (targetToReplace != null) {
      outputs.get(splitTarget).remove(targetToReplace);
    } else if (srcTarget == null) {
      srcTarget = stream.createTempOutput(splitTarget.getType());
    }
    outputs.get(splitTarget).add(srcTarget);
    splitTarget.materializeAt(srcTarget);

    RDDListImpl<?> inputNode = (RDDListImpl<?>) stream.read(srcTarget);
    Set<NodePath> nextNodePaths = Sets.newHashSet();
    for (NodePath nodePath : currentNodePaths) {
      if (gbkPaths.contains(nodePath))
        nextNodePaths.add(nodePath.splitAt(splitIndex, inputNode));
      else
        nextNodePaths.add(nodePath);

    }
    currentNodePaths.clear();
    currentNodePaths.addAll(nextNodePaths);
  }

  private Set<GroupedPairList<?, ?>> getWorkingGroupings(Map<RDDListImpl<?>, Set<NodePath>> nodePaths) {
    Set<GroupedPairList<?, ?>> gbks = Sets.newHashSet();
    for (RDDListImpl<?> target : nodePaths.keySet()) {
      if (target instanceof GroupedPairList) {
        boolean hasGBKDependency = false;
        for (NodePath nodePath : nodePaths.get(target)) {
          if (nodePath.head() instanceof GroupedPairList) {
            hasGBKDependency = true;
            break;
          }
        }
        if (!hasGBKDependency) {
          gbks.add((GroupedPairList<?, ?>) target);
        }
      }
    }
    return gbks;
  }

  private static class NodeVisitor implements RDDListImpl.Visitor {

    private final Map<RDDListImpl<?>, Set<NodePath>> nodePaths;
    private RDDListImpl<?> workingNode;
    private NodePath workingPath;

    public NodeVisitor() {
      this.nodePaths = new HashMap<>();
    }

    public Map<RDDListImpl<?>, Set<NodePath>> getNodePaths() {
      return nodePaths;
    }

    public void visitOutput(RDDListImpl<?> output) {
      nodePaths.put(output, Sets.newHashSet());
      workingNode = output;
      workingPath = new NodePath();
      output.accept(this);
    }

    @Override
    public void visitInputCollection(InputList<?> collection) {
      workingPath.close(collection);
      nodePaths.get(workingNode).add(workingPath);
    }

    @Override
    public void visitUnionCollection(UnionList<?> collection) {
      RDDListImpl<?> baseNode = workingNode;
      NodePath basePath = workingPath;
      for (RDDListImpl<?> d : collection.getDependencies()) {
        workingPath = new NodePath(basePath);
        workingNode = baseNode;
        processDependency(d);
      }
    }

    @Override
    public void visitTransformList(TransformListImpl<?> collection) {
      workingPath.push(collection);
      processDependency(collection.getOnlyDependency());
    }

    @Override
    public void visitTransformPairList(TransformPairListImpl<?, ?> collection) {
      workingPath.push(collection);
      processDependency(collection.getOnlyDependency());
    }

    @Override
    public void visitTransformGroupedPairList(GroupedPairList<?, ?> collection) {
      workingPath.close(collection);
      nodePaths.get(workingNode).add(workingPath);
      workingNode = collection;
      nodePaths.put(workingNode, Sets.newHashSet());
      workingPath = new NodePath(collection);
      processDependency(collection.getOnlyDependency());
    }

    private void processDependency(RDDListImpl<?> dependency) {
      if (!nodePaths.containsKey(dependency)) {
        dependency.accept(this);
      } else {
        workingPath.close(dependency);
        nodePaths.get(workingNode).add(workingPath);
      }
    }
  }
}
