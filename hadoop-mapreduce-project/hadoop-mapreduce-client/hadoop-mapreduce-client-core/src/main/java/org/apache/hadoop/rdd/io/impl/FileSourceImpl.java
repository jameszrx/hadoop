
package org.apache.hadoop.rdd.io.impl;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.rdd.io.FileHelper;
import org.apache.hadoop.rdd.io.Source;
import org.apache.hadoop.rdd.types.Type;

import java.io.IOException;

public abstract class FileSourceImpl<T> implements Source<T> {


  protected final Path path;
  protected final Type<T> type;
  protected final InputBundle inputBundle;

  public FileSourceImpl(Path path, Type<T> type, Class<? extends InputFormat> inputFormatClass) {
    this.path = path;
    this.type = type;
    this.inputBundle = new InputBundle(inputFormatClass);
  }


  @Override
  public void configureSource(Job job) throws IOException {
      FileInputFormat.addInputPath(job, path);
      job.setInputFormatClass(inputBundle.getInputFormatClass());
      inputBundle.configure(job.getConfiguration());
  }

  @Override
  public Type<T> getType() {
    return type;
  }

  @Override
  public long getSize(Configuration configuration) {
    try {
      return FileHelper.getPathSize(configuration, path);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return -1;
  }

//  @Override
//  public boolean equals(Object other) {
//    if (other == null || !getClass().equals(other.getClass())) {
//      return false;
//    }
//    FileSourceImpl o = (FileSourceImpl) other;
//    return type.equals(o.type) && path.equals(o.path) && inputBundle.equals(o.inputBundle);
//  }


}
