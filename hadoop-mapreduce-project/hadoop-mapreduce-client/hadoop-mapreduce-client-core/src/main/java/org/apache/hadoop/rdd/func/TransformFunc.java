
package org.apache.hadoop.rdd.func;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.rdd.io.save.Saver;

import java.io.Serializable;


public abstract class TransformFunc<S, T> implements Serializable {

  private transient TaskInputOutputContext<?, ?, ?, ?> context;

  public void configure(Configuration conf) {
  }


  public abstract void process(S input, Saver<T> saver);


  public void init() {
  }


  public void cleanup(Saver<T> saver) {
  }


  public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
    this.context = context;
    init();
  }

  protected TaskInputOutputContext<?, ?, ?, ?> getContext() {
    return context;
  }

}
