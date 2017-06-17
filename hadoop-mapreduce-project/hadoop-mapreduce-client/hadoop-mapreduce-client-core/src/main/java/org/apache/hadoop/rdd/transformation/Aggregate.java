
package org.apache.hadoop.rdd.transformation;

import org.apache.hadoop.rdd.func.CombineFunc;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.func.TransformFunc;
import org.apache.hadoop.rdd.io.save.Saver;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.structure.RDDList;
import org.apache.hadoop.rdd.structure.RDDPairList;
import org.apache.hadoop.rdd.types.writable.Writables;


public class Aggregate {


  public static <S> RDDPairList<S, Long> count(RDDList<S> collect) {

    return collect.parallel("Aggregate.count", new MapFunc<S, Pair<S, Long>>() {
      public Pair<S, Long> map(S input) {
        return new Pair<>(input, 1L);
      }
    }, Writables.tableOf(collect.getType(), Writables.longs())).groupByKey().combineValues(CombineFunc.SUM_LONGS());
  }



  public static <S> RDDList<S> max(RDDList<S> collect) {
    Class<S> clazz = collect.getType().getTypeClass();
    if (!clazz.isPrimitive() && !Comparable.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException("Can only get max for Comparable elements, not for: "
          + collect.getType().getTypeClass());
    }

    return RDDPairLists.values(collect.parallel("max", new TransformFunc<S, Pair<Boolean, S>>() {
      private transient S max = null;

      public void process(S input, Saver<Pair<Boolean, S>> saver) {
        if (max == null || ((Comparable<S>) max).compareTo(input) < 0) {
          max = input;
        }
      }

      public void cleanup(Saver<Pair<Boolean, S>> saver) {
        if (max != null) {
          saver.save(new Pair<>(true, max));
        }
      }
    }, Writables.tableOf(Writables.booleans(), collect.getType())).groupByKey().combineValues(new CombineFunc<Boolean, S>() {
      public void process(Pair<Boolean, Iterable<S>> input, Saver<Pair<Boolean, S>> saver) {
        S max = null;
        for (S v : input.value()) {
          if (max == null || ((Comparable<S>) max).compareTo(v) < 0) {
            max = v;
          }
        }
        saver.save(new Pair<>(input.key(), max));
      }
    }));
  }


  public static <S> RDDList<S> min(RDDList<S> collect) {
    return RDDPairLists.values(collect.parallel("min", new TransformFunc<S, Pair<Boolean, S>>() {
      private transient S min = null;

      public void process(S input, Saver<Pair<Boolean, S>> saver) {
        if (min == null || ((Comparable<S>) min).compareTo(input) > 0) {
          min = input;
        }
      }

      public void cleanup(Saver<Pair<Boolean, S>> saver) {
        if (min != null) {
          saver.save(new Pair<>(false, min));
        }
      }
    }, Writables.tableOf(Writables.booleans(), collect.getType())).groupByKey().combineValues(new CombineFunc<Boolean, S>() {
      public void process(Pair<Boolean, Iterable<S>> input, Saver<Pair<Boolean, S>> saver) {
        S min = null;
        for (S v : input.value()) {
          if (min == null || ((Comparable<S>) min).compareTo(v) > 0) {
            min = v;
          }
        }
        saver.save(new Pair<>(input.key(), min));
      }
    }));
  }

}
