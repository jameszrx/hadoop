
package org.apache.hadoop.rdd.io.seq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.rdd.io.PathIterable;
import org.apache.hadoop.rdd.io.ReadableSource;
import org.apache.hadoop.rdd.io.impl.FileSourceImpl;
import org.apache.hadoop.rdd.types.Type;

import java.io.IOException;

public class SeqFileSource<T> extends FileSourceImpl<T> implements ReadableSource<T> {

  public SeqFileSource(Path path, Type<T> ptype) {
    super(path, ptype, SequenceFileInputFormat.class);
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return PathIterable.create(fs, path, new SeqFileReaderFactory<T>(type, conf));
  }

  @Override
  public String toString() {
    return "SeqFile(" + path.toString() + ")";
  }
}
