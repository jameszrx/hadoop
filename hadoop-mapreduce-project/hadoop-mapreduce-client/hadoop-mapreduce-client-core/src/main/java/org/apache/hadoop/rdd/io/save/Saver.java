
package org.apache.hadoop.rdd.io.save;


public interface  Saver<T> {
  void save(T saved);
}
