
package org.apache.hadoop.rdd.types;

import org.apache.hadoop.rdd.structure.Pair;

import java.io.Serializable;

public abstract class PairFactory implements Serializable {

  public void initialize() {
  }

  public abstract Pair makePair(Object... values);

  public static final PairFactory PAIR = new PairFactory() {
    @Override
    public Pair makePair(Object... values) {
      return new Pair<>(values[0], values[1]);
    }
  };


}
