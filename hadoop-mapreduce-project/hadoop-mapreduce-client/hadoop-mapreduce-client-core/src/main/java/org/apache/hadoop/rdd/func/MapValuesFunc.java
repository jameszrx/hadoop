
package org.apache.hadoop.rdd.func;

import org.apache.hadoop.rdd.io.save.Saver;
import org.apache.hadoop.rdd.structure.Pair;

public abstract class MapValuesFunc<K, V1, V2> extends TransformFunc<Pair<K, V1>, Pair<K, V2>> {

  @Override
  public void process(Pair<K, V1> input, Saver<Pair<K, V2>> saver) {
    saver.save(new Pair<>(input.key(), map(input.value())));
  }

  public abstract V2 map(V1 v);
}
