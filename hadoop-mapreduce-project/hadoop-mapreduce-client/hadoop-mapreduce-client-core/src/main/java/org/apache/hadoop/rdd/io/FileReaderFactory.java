
package org.apache.hadoop.rdd.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Iterator;

public interface FileReaderFactory<T> {
  Iterator<T> read(FileSystem fs, Path path);
}
