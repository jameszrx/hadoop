
package org.apache.hadoop.rdd.io;

import org.apache.hadoop.rdd.plan.OutputHandler;
import org.apache.hadoop.rdd.types.Type;


public interface Target {
  boolean accept(OutputHandler handler, Type<?> ptype);

  <T> SourceTarget<T> asSourceTarget(Type<T> ptype);
}
