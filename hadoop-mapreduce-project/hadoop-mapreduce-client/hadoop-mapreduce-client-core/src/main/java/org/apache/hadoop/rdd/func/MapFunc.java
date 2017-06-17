
package org.apache.hadoop.rdd.func;


import org.apache.hadoop.rdd.io.save.Saver;

public abstract class MapFunc<S, T> extends TransformFunc<S, T> {


  public abstract T map(S input);

  @Override
  public void process(S input, Saver<T> saver) {
    saver.save(map(input));
  }

//  @Override
//  public float scaleFactor() {
//    return 1.0f;
//  }
}
