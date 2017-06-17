
package org.apache.hadoop.rdd.io.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.rdd.io.PathIterable;
import org.apache.hadoop.rdd.io.ReadableSource;
import org.apache.hadoop.rdd.io.impl.FileSourceImpl;
import org.apache.hadoop.rdd.types.Type;

import java.io.IOException;

public class TextFileSource<T> extends FileSourceImpl<T> implements ReadableSource<T> {


  public TextFileSource(Path path, Type<T> ptype) {
    super(path, ptype,TextInputFormat.class);
  }

  @Override
  public long getSize(Configuration conf) {

    return super.getSize(conf);
  }


  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    return PathIterable.create(path.getFileSystem(conf), path, new TextFileReaderFactory<T>());
  }
}
