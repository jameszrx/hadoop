
package org.apache.hadoop.rdd.types;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.io.SourceTarget;
import org.apache.hadoop.rdd.structure.Pair;

import java.util.Iterator;
import java.util.List;


public abstract class GroupedPairListType<K, V> implements Type<Pair<K, Iterable<V>>> {

  protected static class PTypeIterable<V> implements Iterable<V> {
    private final Iterable<Object> iterable;
    private final MapFunc<Object, V> mapFunc;

    public PTypeIterable(MapFunc<Object, V> mapFunc, Iterable<Object> iterable) {
      this.mapFunc = mapFunc;
      this.iterable = iterable;
    }

    public Iterator<V> iterator() {
      return new Iterator<V>() {
        Iterator<Object> iter = iterable.iterator();

        public boolean hasNext() {
          return iter.hasNext();
        }

        public V next() {
          return mapFunc.map(iter.next());
        }

        public void remove() {
          iter.remove();
        }
      };
    }
  }

  public static class PairIterableMapFunc<K, V> extends MapFunc<Pair<Object, Iterable<Object>>, Pair<K, Iterable<V>>> {
    private final MapFunc<Object, K> keys;
    private final MapFunc<Object, V> values;

    public PairIterableMapFunc(MapFunc<Object, K> keys, MapFunc<Object, V> values) {
      this.keys = keys;
      this.values = values;
    }

    @Override
    public void init() {
      keys.init();
      values.init();
    }

    @Override
    public Pair<K, Iterable<V>> map(Pair<Object, Iterable<Object>> input) {
      return new Pair<>(keys.map(input.key()), new PTypeIterable(values, input.value()));
    }
  }

  protected final RDDPairListType<K, V> tableType;

  public GroupedPairListType(RDDPairListType<K, V> tableType) {
    this.tableType = tableType;
  }

  public RDDPairListType<K, V> getPairListType() {
    return tableType;
  }






  @Override
  public List<Type> getSubTypes() {
    return tableType.getSubTypes();
  }

  @Override
  public Converter getConverter() {
    return tableType.getConverter();
  }

  public abstract Converter getGroupingConverter();

  public abstract void configureShuffle(Job job);

  @Override
  public SourceTarget<Pair<K, Iterable<V>>> getDefaultFileSource(Path path) {
    return null;
  }
}
