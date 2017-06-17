
package org.apache.hadoop.rdd.io.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.rdd.io.PathTarget;
import org.apache.hadoop.rdd.io.Source;
import org.apache.hadoop.rdd.types.Type;

public class SourcePathTargetImpl<T> extends SourceTargetImpl<T> implements PathTarget {

  public SourcePathTargetImpl(Source<T> source, PathTarget target) {
    super(source, target);
  }

  @Override
  public void configureHadoop(Job job, Type<?> ptype, Path outputPath, String name) {
    ((PathTarget) target).configureHadoop(job, ptype, outputPath, name);
  }

  @Override
  public Path getPath() {
    return ((PathTarget) target).getPath();
  }
}
