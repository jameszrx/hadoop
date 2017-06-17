
package org.apache.hadoop.rdd.io.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.rdd.io.ReadableSource;
import org.apache.hadoop.rdd.io.ReadableSourceTarget;
import org.apache.hadoop.rdd.io.impl.SourcePathTargetImpl;
import org.apache.hadoop.rdd.types.Type;

import java.io.IOException;

public class TextFileSourceTarget<T> extends SourcePathTargetImpl<T> implements ReadableSourceTarget<T> {

  public TextFileSourceTarget(Path path, Type<T> ptype) {
    super(new TextFileSource<T>(path, ptype), new TextFileTarget(path));
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
        return ((ReadableSource<T>) source).read(conf);
  }
}
