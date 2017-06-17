
package org.apache.hadoop.rdd.io.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.rdd.io.PathTarget;
import org.apache.hadoop.rdd.io.ReadableSource;
import org.apache.hadoop.rdd.io.ReadableSourceTarget;

import java.io.IOException;

public class ReadableSourcePathTargetImpl<T> extends SourcePathTargetImpl<T> implements ReadableSourceTarget<T> {

  public ReadableSourcePathTargetImpl(ReadableSource<T> source, PathTarget target) {
    super(source, target);
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    return ((ReadableSource<T>) source).read(conf);
  }

}
