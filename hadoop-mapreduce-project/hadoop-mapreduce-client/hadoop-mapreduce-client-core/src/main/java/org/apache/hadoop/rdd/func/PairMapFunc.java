
package org.apache.hadoop.rdd.func;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.rdd.io.save.Saver;
import org.apache.hadoop.rdd.structure.Pair;

public class PairMapFunc<K, V, S, T> extends MapFunc<Pair<K, V>, Pair<S, T>> {

  private MapFunc<K, S> keys;
  private MapFunc<V, T> values;

  public PairMapFunc(MapFunc<K, S> keys, MapFunc<V, T> values) {
    this.keys = keys;
    this.values = values;
  }

  @Override
  public void configure(Configuration conf) {
    keys.configure(conf);
    values.configure(conf);
  }

  @Override
  public void init() {
    keys.setContext(getContext());
    values.setContext(getContext());
  }

  @Override
  public Pair<S, T> map(Pair<K, V> input) {
    return new Pair<>(keys.map(input.key()), values.map(input.value()));
  }

  @Override
  public void cleanup(Saver<Pair<S, T>> saver) {
    keys.cleanup(null);
    values.cleanup(null);
  }

}