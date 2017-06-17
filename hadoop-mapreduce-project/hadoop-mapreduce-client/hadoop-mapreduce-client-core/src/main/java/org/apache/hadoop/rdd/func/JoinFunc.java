
package org.apache.hadoop.rdd.func;

import com.google.common.collect.Lists;
import org.apache.hadoop.rdd.io.save.Saver;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.types.Type;

import java.util.List;


public class JoinFunc<K, U, V> extends TransformFunc<Pair<Pair<K, Integer>, Iterable<Pair<U, V>>>, Pair<K, Pair<U, V>>> {

  private Type<K> keyType;
  private Type<U> leftType;

  private transient K lastKey;
  private transient List<U> lefts;


  public JoinFunc(Type<K> keyType, Type<U> leftType) {
    this.keyType = keyType;
    this.leftType = leftType;
  }

  @Override
  public void init() {
    lastKey = null;
    this.lefts = Lists.newArrayList();
  }


  public void join(K key, int id, Iterable<Pair<U, V>> pairs, Saver<Pair<K, Pair<U, V>>> saver) {
    if (!key.equals(lastKey)) {
      lastKey = keyType.getDetachedValue(key);
      lefts.clear();
    }
    if (id == 0) {
      for (Pair<U, V> pair : pairs) {
        if (pair.key() != null)
          lefts.add(leftType.getDetachedValue(pair.key()));
      }
    } else {
      for (Pair<U, V> pair : pairs) {
        for (U u : lefts) {
          saver.save(new Pair<>(lastKey, new Pair<>(u, pair.value())));
        }
      }
    }
  }

  @Override
  public void process(Pair<Pair<K, Integer>, Iterable<Pair<U, V>>> input, Saver<Pair<K, Pair<U, V>>> saver) {
    join(input.key().key(), input.key().value(), input.value(), saver);
  }
}
