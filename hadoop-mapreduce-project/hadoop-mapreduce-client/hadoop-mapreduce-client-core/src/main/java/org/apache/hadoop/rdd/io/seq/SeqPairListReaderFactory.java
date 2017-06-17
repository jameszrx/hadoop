
package org.apache.hadoop.rdd.io.seq;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.io.FileReaderFactory;
import org.apache.hadoop.rdd.io.impl.ReaderIterator;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.types.RDDPairListType;
import org.apache.hadoop.rdd.types.Type;

import java.io.IOException;
import java.util.Iterator;

public class SeqPairListReaderFactory<K, V> implements FileReaderFactory<Pair<K, V>> {

  private final MapFunc<Object, K> keyMapFunc;
  private final MapFunc<Object, V> valueMapFunc;
  private final Writable key;
  private final Writable value;
  private final Configuration conf;

  public SeqPairListReaderFactory(RDDPairListType<K, V> tableType, Configuration conf) {
    Type<K> keyType = tableType.getKeyType();
    Type<V> valueType = tableType.getValueType();
    this.keyMapFunc = SeqFileHelper.getInputMapFunc(keyType);
    this.valueMapFunc = SeqFileHelper.getInputMapFunc(valueType);
    this.key = SeqFileHelper.newInstance(keyType, conf);
    this.value = SeqFileHelper.newInstance(valueType, conf);
    this.conf = conf;
  }

  @Override
  public Iterator<Pair<K, V>> read(FileSystem fs, final Path path) {

    keyMapFunc.init();

    valueMapFunc.init();
    try {
      final SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
      return new ReaderIterator<>(reader, new UnmodifiableIterator<Pair<K, V>>() {
        boolean nextChecked = false;
        boolean hasNext = false;

        @Override
        public boolean hasNext() {
          if (nextChecked) {
            return hasNext;
          }
          try {
            hasNext = reader.next(key, value);
          } catch (IOException e) {
            e.printStackTrace();
            return false;
          }
          nextChecked = true;
          return hasNext;
        }

        @Override
        public Pair<K, V> next() {
          if (!nextChecked && !hasNext()) {
            return null;
          }
          nextChecked = false;
          return new Pair<>(keyMapFunc.map(key), valueMapFunc.map(value));
        }
      });
    } catch (IOException e) {
      e.printStackTrace();
      return Iterators.emptyIterator();
    }
  }
}
