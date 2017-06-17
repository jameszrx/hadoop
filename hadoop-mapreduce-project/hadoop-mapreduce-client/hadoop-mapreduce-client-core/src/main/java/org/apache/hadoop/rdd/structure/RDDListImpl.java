
package org.apache.hadoop.rdd.structure;

import com.google.common.collect.Lists;
import org.apache.hadoop.rdd.func.CombineFunc;
import org.apache.hadoop.rdd.func.FilterFunc;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.func.TransformFunc;
import org.apache.hadoop.rdd.io.SourceTarget;
import org.apache.hadoop.rdd.io.Target;
import org.apache.hadoop.rdd.job.Stream;
import org.apache.hadoop.rdd.plan.TransformNode;
import org.apache.hadoop.rdd.transformation.Sort;
import org.apache.hadoop.rdd.types.RDDPairListType;
import org.apache.hadoop.rdd.types.Type;
import org.apache.hadoop.rdd.types.writable.Writables;

import java.util.Collections;
import java.util.List;

public abstract class RDDListImpl<S> implements RDDList<S> {

  private final String name;
  protected Stream stream;
  private SourceTarget<S> materializedAt;

  public RDDListImpl(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public RDDList<S> union(RDDList<S>... collections) {
    List<RDDListImpl<S>> internal = Lists.newArrayList();
    internal.add(this);
    for (RDDList<S> collection : collections) {
      internal.add((RDDListImpl<S>) collection);
    }
    return new UnionList<S>(internal);
  }

  @Override
  public <T> RDDList<T> parallel(TransformFunc<S, T> func, Type<T> type) {
    Stream s = (Stream) getStream();
    return parallel("S" + s.getNextAnonymousStageId(), func, type);
  }

  @Override
  public <T> RDDList<T> parallel(String name, TransformFunc<S, T> func, Type<T> type) {
    return new TransformListImpl<T>(name, this, func, type);
  }

  @Override
  public <K, V> RDDPairList<K, V> parallel(TransformFunc<S, Pair<K, V>> func, RDDPairListType<K, V> type) {
    Stream s = (Stream) getStream();
    return parallel("S" + s.getNextAnonymousStageId(), func, type);
  }

  @Override
  public <K, V> RDDPairList<K, V> parallel(String name, TransformFunc<S, Pair<K, V>> func, RDDPairListType<K, V> type) {
    return new TransformPairListImpl<K, V>(name, this, func, type);
  }

  @Override
  public RDDList<S> write(Target target) {
    getStream().write(this, target);
    return this;
  }

  @Override
  public Iterable<S> materialize() {
    if (getSize() == 0) {
      return Collections.emptyList();
    }
    return getStream().materialize(this);
  }

  public SourceTarget<S> getMaterializedAt() {
    return materializedAt;
  }

  public void materializeAt(SourceTarget<S> sourceTarget) {
    this.materializedAt = sourceTarget;
  }

  @Override
  public RDDList<S> filter(FilterFunc<S> filterFunc) {
    return parallel(filterFunc, getType());
  }

  @Override
  public RDDList<S> filter(String name, FilterFunc<S> filterFunc) {
    return parallel(name, filterFunc, getType());
  }


  @Override
  public RDDList<S> sort(boolean ascending) {
    return Sort.sort(this);
  }

  @Override
  public RDDPairList<S, Long> count() {
    return this.parallel("Aggregate.count", new MapFunc<S, Pair<S, Long>>() {
      public Pair<S, Long> map(S input) {
        return new Pair<>(input, 1L);
      }
    }, Writables.tableOf(this.getType(), Writables.longs())).groupByKey().combineValues(CombineFunc.SUM_LONGS());

  }


  public abstract TransformNode createTransformNode();

  public abstract List<RDDListImpl<?>> getDependencies();

  public RDDListImpl<?> getOnlyDependency() {
    List<RDDListImpl<?>> ds = getDependencies();
    return ds.get(0);
  }

  @Override
  public Stream getStream() {
    if (stream == null) {
      stream = (Stream) getDependencies().get(0).getStream();
    }
    return stream;
  }

  public int getDepth() {
    int m = 0;
    for (RDDListImpl d : getDependencies()) {
      m = Math.max(d.getDepth(), m);
    }
    return 1 + m;
  }

  public interface Visitor {
    void visitInputCollection(InputList<?> collection);

    void visitUnionCollection(UnionList<?> collection);

    void visitTransformList(TransformListImpl<?> collection);

    void visitTransformPairList(TransformPairListImpl<?, ?> collection);

    void visitTransformGroupedPairList(GroupedPairList<?, ?> collection);
  }

  public void accept(Visitor visitor) {
    if (materializedAt != null) {
      visitor.visitInputCollection(new InputList<S>(materializedAt, (Stream) getStream()));
    } else {
      acceptPrivate(visitor);
    }
  }

  protected abstract void acceptPrivate(Visitor visitor);

  @Override
  public long getSize() {
    if (materializedAt != null) {
      long sz = materializedAt.getSize(getStream().getConfiguration());
      if (sz > 0) {
        return sz;
      }
    }
    return size();
  }

  protected abstract long size();
}
