
package org.apache.hadoop.rdd.func;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.rdd.io.save.Saver;

public class CompositeMapFunc<R, S, T> extends MapFunc<R, T> {

  private final MapFunc<R, S> key;
  private final MapFunc<S, T> second;

  public CompositeMapFunc(MapFunc<R, S> key, MapFunc<S, T> second) {
    this.key = key;
    this.second = second;
  }

  public MapFunc<R, S> getKey() {
    return key;
  }


  @Override
  public void init() {
    key.setContext(getContext());
    second.setContext(getContext());
  }

  public MapFunc<S, T> getSecond() {
    return second;
  }

  @Override
  public T map(R input) {
    return second.map(key.map(input));
  }

  @Override
  public void cleanup(Saver<T> saver) {
    key.cleanup(null);
    second.cleanup(null);
  }

  @Override
  public void configure(Configuration conf) {
    key.configure(conf);
    second.configure(conf);
  }

}
