
package org.apache.hadoop.rdd.func;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.rdd.io.save.Saver;
import org.apache.hadoop.rdd.structure.Pair;

import java.io.Serializable;


public abstract class CombineFunc<S, T> extends TransformFunc<Pair<S, Iterable<T>>, Pair<S, T>> {

  public interface Aggregator<T> extends Serializable {
    void reset();
    void update(T value);
    Iterable<T> results();
  }


  public interface AggregatorFactory<T> {
    Aggregator<T> create();
  }


  public static class AggregatorCombineFunc<K, V> extends CombineFunc<K, V> {

    private final Aggregator<V> aggregator;

    public AggregatorCombineFunc(Aggregator<V> aggregator) {
      this.aggregator = aggregator;
    }

    @Override
    public void process(Pair<K, Iterable<V>> input, Saver<Pair<K, V>> saver) {
      aggregator.reset();
      for (V v : input.value()) {
        aggregator.update(v);
      }
      for (V v : aggregator.results()) {
        saver.save(new Pair<>(input.key(), v));
      }
    }
  }

  public static <K, V> CombineFunc<K, V> aggregator(Aggregator<V> aggregator) {
    return new AggregatorCombineFunc<>(aggregator);
  }

  public static <K> CombineFunc<K, Long> SUM_LONGS() {
    return new AggregatorCombineFunc<>(SUM_LONGS.create());
  }


  public static class SumLongs implements Aggregator<Long> {
    private long sum = 0;

    @Override
    public void reset() {
      sum = 0;
    }

    @Override
    public void update(Long next) {
      sum += next;
    }

    @Override
    public Iterable<Long> results() {
      return ImmutableList.of(sum);
    }
  }

  private static AggregatorFactory<Long> SUM_LONGS = SumLongs::new;



}
