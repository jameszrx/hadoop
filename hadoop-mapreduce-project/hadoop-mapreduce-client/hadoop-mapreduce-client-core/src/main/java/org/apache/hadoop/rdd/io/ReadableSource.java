
package org.apache.hadoop.rdd.io;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public interface ReadableSource<T> extends Source<T> {
  Iterable<T> read(Configuration conf) throws IOException;
}
