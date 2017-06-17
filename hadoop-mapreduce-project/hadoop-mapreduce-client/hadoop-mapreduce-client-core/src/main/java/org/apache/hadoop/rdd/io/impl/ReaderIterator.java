
package org.apache.hadoop.rdd.io.impl;

import com.google.common.collect.UnmodifiableIterator;
import com.google.common.io.Closeables;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;


public class ReaderIterator<T> extends UnmodifiableIterator<T> implements Iterator<T>, Closeable {
  private final Iterator<T> iter;
  private Closeable closeable;

  public ReaderIterator(Closeable closeable, Iterator<T> iter) {
    this.closeable = closeable;
    this.iter = iter;
  }

  @Override
  public boolean hasNext() {
    if (!iter.hasNext()) {
      Closeables.closeQuietly(this);
      return false;
    } else {
      return true;
    }
  }

  @Override
  public T next() {
    return iter.next();
  }

  @Override
  public void close() throws IOException {
    if (closeable != null) {
      closeable.close();
      closeable = null;
    }
  }
}
