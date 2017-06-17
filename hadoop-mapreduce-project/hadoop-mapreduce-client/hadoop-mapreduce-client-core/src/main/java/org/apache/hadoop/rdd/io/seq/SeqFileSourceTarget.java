
package org.apache.hadoop.rdd.io.seq;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.rdd.io.impl.ReadableSourcePathTargetImpl;
import org.apache.hadoop.rdd.types.Type;

public class SeqFileSourceTarget<T> extends ReadableSourcePathTargetImpl<T> {




  public SeqFileSourceTarget(Path path, Type<T> ptype) {
    super(new SeqFileSource<T>(path, ptype), new SeqFileTarget(path));
  }

  @Override
  public String toString() {
    return target.toString();
  }
}
