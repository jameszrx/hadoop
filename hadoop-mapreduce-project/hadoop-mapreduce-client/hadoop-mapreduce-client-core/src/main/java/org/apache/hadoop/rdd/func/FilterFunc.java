
package org.apache.hadoop.rdd.func;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.rdd.io.save.Saver;

import java.util.List;


public abstract class FilterFunc<T> extends TransformFunc<T, T> {


  public abstract boolean accept(T input);

  @Override
  public void process(T input, Saver<T> saver) {
    if (accept(input)) {
      saver.save(input);
    }
  }


  public static <S> FilterFunc<S> and(FilterFunc<S>... funcs) {
    return new AndFunc<S>(funcs);
  }

  public static class AndFunc<S> extends FilterFunc<S> {

    private final List<FilterFunc<S>> funcs;

    AndFunc(FilterFunc<S>... funcs) {
      this.funcs = ImmutableList.copyOf(funcs);
    }

    @Override
    public boolean accept(S input) {
      for (FilterFunc<S> f : funcs) {
        if (!f.accept(input)) {
          return false;
        }
      }
      return true;
    }

  }

  public static <S> FilterFunc<S> or(FilterFunc<S>... funcs) {
    return new OrFunc<S>(funcs);
  }

  public static class OrFunc<S> extends FilterFunc<S> {

    private final List<FilterFunc<S>> funcs;

    OrFunc(FilterFunc<S>... funcs) {
      this.funcs = ImmutableList.copyOf(funcs);
    }

    @Override
    public boolean accept(S input) {
      for (FilterFunc<S> f : funcs) {
        if (f.accept(input)) {
          return true;
        }
      }
      return false;
    }


  }

  public static <S> FilterFunc<S> not(FilterFunc<S> f) {
    return new NotFunc<S>(f);
  }

  public static class NotFunc<S> extends FilterFunc<S> {

    private final FilterFunc<S> base;

    NotFunc(FilterFunc<S> base) {
      this.base = base;
    }

    @Override
    public boolean accept(S input) {
      return !base.accept(input);
    }

  }
}
