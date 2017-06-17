
package org.apache.hadoop.rdd.io.seq;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.rdd.io.SourceTarget;
import org.apache.hadoop.rdd.io.impl.FileTargetImpl;
import org.apache.hadoop.rdd.types.RDDPairListType;
import org.apache.hadoop.rdd.types.Type;

public class SeqFileTarget extends FileTargetImpl {




  public SeqFileTarget(Path path) {
    super(path, SequenceFileOutputFormat.class);
  }

  @Override
  public String toString() {
    return "SeqFile(" + path.toString() + ")";
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(Type<T> ptype) {
    if (ptype instanceof RDDPairListType) {
      return new SeqPairListSourceTarget(path, (RDDPairListType) ptype);
    } else {
      return new SeqFileSourceTarget(path, ptype);
    }
  }
}
