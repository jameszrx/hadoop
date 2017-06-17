
package org.apache.hadoop.rdd.io.text;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.func.SelfFunc;
import org.apache.hadoop.rdd.io.FileReaderFactory;
import org.apache.hadoop.rdd.io.impl.ReaderIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

public class TextFileReaderFactory<T> implements FileReaderFactory<T> {

  private static final Log LOG = LogFactory.getLog(TextFileReaderFactory.class);

  @Override
  public Iterator<T> read(FileSystem fs, Path path) {
    MapFunc mapFunc = SelfFunc.getInstance();

    mapFunc.init();

    FSDataInputStream is;
    try {
      is = fs.open(path);
    } catch (IOException e) {
      LOG.info("Could not read path: " + path, e);
      return Iterators.emptyIterator();
    }

    final BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    final MapFunc<String, T> iterMapFunc = mapFunc;
    return new ReaderIterator<T>(reader, new UnmodifiableIterator<T>() {
      private String nextLine;

      @Override
      public boolean hasNext() {
        try {
          return (nextLine = reader.readLine()) != null;
        } catch (IOException e) {
          LOG.info("Exception reading text file stream", e);
          return false;
        }
      }

      @Override
      public T next() {
        return iterMapFunc.map(nextLine);
      }
    });
  }
}
