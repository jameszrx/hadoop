
package org.apache.hadoop.rdd.io;

import org.apache.hadoop.fs.Path;

public interface PathTarget extends MapReduceTarget {
  Path getPath();
}
