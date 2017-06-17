
package org.apache.hadoop.rdd.func;

public class SelfFunc<T> extends MapFunc<T, T> {

  private static final SelfFunc<Object> instance = new SelfFunc<>();

  public static <T> SelfFunc<T> getInstance() {
    return (SelfFunc<T>) instance;
  }


  private SelfFunc() {
  }

  @Override
  public T map(T input) {
    return input;
  }
}
