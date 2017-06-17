
package org.apache.hadoop.rdd.io.seq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.rdd.io.PathIterable;
import org.apache.hadoop.rdd.io.ReadableSource;
import org.apache.hadoop.rdd.io.impl.PairListSourceImpl;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.types.RDDPairListType;

import java.io.IOException;


public class SeqPairListSource<K, V> extends PairListSourceImpl<K, V> implements ReadableSource<Pair<K, V>> {

  public SeqPairListSource(Path path, RDDPairListType<K, V> ptype) {
    super(path, ptype, SequenceFileInputFormat.class);
  }

  @Override
  public Iterable<Pair<K, V>> read(Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return PathIterable.create(fs, path, new SeqPairListReaderFactory<K, V>((RDDPairListType<K, V>) type, conf));
  }

}
