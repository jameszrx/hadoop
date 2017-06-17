
package org.apache.hadoop.rdd.structure;

public class Pair<K, V> implements Comparable<Pair<K, V>> {

  private final K key;
  private final V value;

  public Pair(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public K key() {
    return key;
  }

  public V value() {
    return value;
  }

  public Object get(int index) {
    if (index == 0) {
      return key;
    }
    else {
      return value;
    }
  }

  public int size() {
    return 2;
  }

  private int cmp(Object lhs, Object rhs) {
    if (lhs == rhs) {
      return 0;
    } else if (lhs != null && Comparable.class.isAssignableFrom(lhs.getClass())) {
      return ((Comparable) lhs).compareTo(rhs);
    }
    return (lhs == null ? 0 : lhs.hashCode()) - (rhs == null ? 0 : rhs.hashCode());
  }

  @Override
  public int compareTo(Pair<K, V> o) {
    int diff = cmp(key, o.key);
    if (diff == 0) {
      diff = cmp(value, o.value);
    }
    return diff;
  }
}
