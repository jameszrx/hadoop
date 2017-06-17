
package org.apache.hadoop.rdd.io;

import com.google.common.collect.UnmodifiableIterator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

public class PathIterable<T> implements Iterable<T> {

  private final FileStatus[] status;
  private final FileSystem fs;
  private final FileReaderFactory<T> readerFactory;


  public static <S> Iterable<S> create(FileSystem fs, Path path, FileReaderFactory<S> readerFactory) throws IOException {

    if (!fs.exists(path)) {
      throw new IOException("files not exists: " + path);
    }

    FileStatus[] status;
    try {
      status = fs.listStatus(path);
    } catch (FileNotFoundException e) {
      status = null;
    }
    if (status == null) {
      throw new IOException("Can't get File Status: " + path);
    }

    if (status.length == 0) {
      return Collections.emptyList();
    } else {
      return new PathIterable<S>(status, fs, readerFactory);
    }

  }

  private PathIterable(FileStatus[] status, FileSystem fs, FileReaderFactory<T> readerFactory) {
    this.status = status;
    this.fs = fs;
    this.readerFactory = readerFactory;
  }

  @Override
  public Iterator<T> iterator() {

    return new UnmodifiableIterator<T>() {
      private int index = 0;
      private Iterator<T> iter = readerFactory.read(fs, status[index++].getPath());

      @Override
      public boolean hasNext() {
        if (!iter.hasNext()) {
          while (index < status.length) {
            iter = readerFactory.read(fs, status[index++].getPath());
            if (iter.hasNext()) {
              return true;
            }
          }
          return false;
        }
        return true;
      }

      @Override
      public T next() {
        return iter.next();
      }
    };
  }
}
