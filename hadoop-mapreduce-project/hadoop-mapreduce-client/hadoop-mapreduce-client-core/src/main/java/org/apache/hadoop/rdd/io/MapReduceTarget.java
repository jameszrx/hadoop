
package org.apache.hadoop.rdd.io;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.rdd.types.Type;

public interface MapReduceTarget extends Target {
  void configureHadoop(Job job, Type<?> ptype, Path outputPath, String name);
}
