
package org.apache.hadoop.rdd.io.seq;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.io.FileReaderFactory;
import org.apache.hadoop.rdd.io.impl.ReaderIterator;
import org.apache.hadoop.rdd.types.Type;

import java.io.IOException;
import java.util.Iterator;

public class SeqFileReaderFactory<T> implements FileReaderFactory<T> {

  private static final Log LOG = LogFactory.getLog(SeqFileReaderFactory.class);

  private final MapFunc<Object, T> mapFunc;
  private final Writable key;
  private final Writable value;
  private final Configuration conf;

  public SeqFileReaderFactory(Type<T> ptype, Configuration conf) {
    this.mapFunc = SeqFileHelper.getInputMapFunc(ptype);
    this.key = NullWritable.get();
    this.value = SeqFileHelper.newInstance(ptype, conf);
    this.conf = conf;
  }

  @Override
  public Iterator<T> read(FileSystem fs, final Path path) {

    mapFunc.init();
    try {
      final SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
      return new ReaderIterator<T>(reader, new UnmodifiableIterator<T>() {
        boolean nextChecked = false;
        boolean hasNext = false;

        @Override
        public boolean hasNext() {
          if (nextChecked) {
            return hasNext;
          }
          try {
            hasNext = reader.next(key, value);
            nextChecked = true;
            return hasNext;
          } catch (IOException e) {
            LOG.info("Error reading from path: " + path, e);
            return false;
          }
        }

        @Override
        public T next() {
          if (!nextChecked && !hasNext()) {
            return null;
          }
          nextChecked = false;
          return mapFunc.map(value);
        }
      });
    } catch (IOException e) {
      LOG.info("Could not read seqfile at path: " + path, e);
      return Iterators.emptyIterator();
    }
  }

}
