
package org.apache.hadoop.rdd.io.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.rdd.io.Source;
import org.apache.hadoop.rdd.io.SourceTarget;
import org.apache.hadoop.rdd.io.Target;
import org.apache.hadoop.rdd.plan.OutputHandler;
import org.apache.hadoop.rdd.types.Type;

import java.io.IOException;

public class SourceTargetImpl<T> implements SourceTarget<T> {

  protected final Source<T> source;
  protected final Target target;

  SourceTargetImpl(Source<T> source, Target target) {
    this.source = source;
    this.target = target;
  }

  @Override
  public Type<T> getType() {
    return source.getType();
  }

  @Override
  public void configureSource(Job job) throws IOException {
    source.configureSource(job);
  }

  @Override
  public long getSize(Configuration configuration) {
    return source.getSize(configuration);
  }

  @Override
  public boolean accept(OutputHandler handler, Type<?> ptype) {
    return target.accept(handler, ptype);
  }

  @Override
  public <S> SourceTarget<S> asSourceTarget(Type<S> ptype) {
    return target.asSourceTarget(ptype);
  }

//  @Override
//  public boolean equals(Object other) {
//    if (other == null || !(other.getClass().equals(getClass()))) {
//      return false;
//    }
//    SourceTargetImpl sti = (SourceTargetImpl) other;
//    return source.equals(sti.source) && target.equals(sti.target);
//  }

}
