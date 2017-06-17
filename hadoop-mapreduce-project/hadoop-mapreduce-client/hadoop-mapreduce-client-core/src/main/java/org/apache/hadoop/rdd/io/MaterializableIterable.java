
package org.apache.hadoop.rdd.io;


import org.apache.hadoop.rdd.job.Stream;

import java.io.IOException;
import java.util.Iterator;

public class MaterializableIterable<E> implements Iterable<E> {

  private final Stream stream;
  private final ReadableSourceTarget<E> sourceTarget;
  private Iterable<E> materialized;

  public MaterializableIterable(Stream stream, ReadableSourceTarget<E> source) {
    this.stream = stream;
    this.sourceTarget = source;
    this.materialized = null;
  }

  public ReadableSourceTarget<E> getSourceTarget() {
    return sourceTarget;
  }

  @Override
  public Iterator<E> iterator() {
    if (materialized == null) {
      stream.run();
      materialize();
    }
    return materialized.iterator();
  }

  public void materialize() {
    try {
      materialized = sourceTarget.read(stream.getConfiguration());
    } catch (IOException e) {

      e.printStackTrace();

    }
  }
}
