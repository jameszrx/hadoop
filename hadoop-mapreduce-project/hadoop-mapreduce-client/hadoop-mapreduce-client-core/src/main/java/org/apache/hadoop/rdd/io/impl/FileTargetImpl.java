
package org.apache.hadoop.rdd.io.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.rdd.io.PathTarget;
import org.apache.hadoop.rdd.io.SourceTarget;
import org.apache.hadoop.rdd.job.output.RDDMultipleOutputs;
import org.apache.hadoop.rdd.plan.OutputHandler;
import org.apache.hadoop.rdd.types.Converter;
import org.apache.hadoop.rdd.types.Type;

public class FileTargetImpl implements PathTarget {

  protected final Path path;
  private final Class<? extends FileOutputFormat> outputFormatClass;

  public FileTargetImpl(Path path, Class<? extends FileOutputFormat> outputFormatClass) {
    this.path = path;
    this.outputFormatClass = outputFormatClass;
  }

  @Override
  public void configureHadoop(Job job, Type<?> type, Path outputPath, String name) {
    Converter converter = type.getConverter();
    Class keyClass = converter.getKeyClass();
    Class valueClass = converter.getValueClass();
    try {
      FileOutputFormat.setOutputPath(job, outputPath);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    RDDMultipleOutputs.addNamedOutput(job, name, outputFormatClass, keyClass, valueClass);
  }

  @Override
  public boolean accept(OutputHandler handler, Type<?> ptype) {
    handler.configure(this, ptype);
    return true;
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(Type<T> ptype) {
    return null;
  }
}
