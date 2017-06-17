
package org.apache.hadoop.rdd.io.text;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.rdd.io.SourceTarget;
import org.apache.hadoop.rdd.io.impl.FileTargetImpl;
import org.apache.hadoop.rdd.types.RDDPairListType;
import org.apache.hadoop.rdd.types.Type;

public class TextFileTarget extends FileTargetImpl {




  public TextFileTarget(Path path) {
    super(path, TextOutputFormat.class);
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return "Text(" + path + ")";
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(Type<T> ptype) {
    if (ptype instanceof RDDPairListType) {
      return null;
    }
    return new TextFileSourceTarget<T>(path, ptype);
  }
}
