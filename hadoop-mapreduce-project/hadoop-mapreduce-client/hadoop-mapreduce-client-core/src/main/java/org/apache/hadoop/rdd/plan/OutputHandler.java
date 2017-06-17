
package org.apache.hadoop.rdd.plan;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.rdd.io.MapReduceTarget;
import org.apache.hadoop.rdd.io.PathTarget;
import org.apache.hadoop.rdd.io.Target;
import org.apache.hadoop.rdd.types.Type;

import java.util.List;

public class OutputHandler {

  private final Job job;
  private final Path path;
  private final boolean mapOnly;

  private TransformNode node;
  private List<Path> paths;

  public OutputHandler(Job job, Path path, boolean mapOnly) {
    this.job = job;
    this.path = path;
    this.mapOnly = mapOnly;
    this.paths = Lists.newArrayList();
  }

  public void configureNode(TransformNode node, Target target) {
    this.node = node;
    target.accept(this, node.getPType());
  }

  public void configure(Target target, Type<?> ptype) {
      String name = "out" + paths.size();
      paths.add(((PathTarget) target).getPath());
      node.setOutputName(name);
      ((MapReduceTarget) target).configureHadoop(job, ptype, path, name);
  }

  public boolean isMapOnly() {
    return mapOnly;
  }

  public List<Path> getPaths() {
    return paths;
  }
}
