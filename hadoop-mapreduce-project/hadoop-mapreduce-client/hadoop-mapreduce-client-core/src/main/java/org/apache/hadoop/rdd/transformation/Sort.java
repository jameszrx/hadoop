
package org.apache.hadoop.rdd.transformation;

import org.apache.hadoop.rdd.func.TransformFunc;
import org.apache.hadoop.rdd.io.save.Saver;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.structure.RDDList;
import org.apache.hadoop.rdd.structure.RDDPairList;
import org.apache.hadoop.rdd.types.RDDPairListType;
import org.apache.hadoop.rdd.types.writable.Writables;


public class Sort {

  public static <T> RDDList<T> sort(RDDList<T> collection) {

    RDDPairListType<T, Void> type = Writables.tableOf(collection.getType(), Writables.nulls());
    RDDPairList<T, Void> pt = collection.parallel(new TransformFunc<T, Pair<T, Void>>() {
      @Override
      public void process(T input, Saver<Pair<T, Void>> emitter) {
        emitter.save(new Pair<>(input, null));
      }
    }, type);
    RDDPairList<T, Void> sortedPt = pt.groupByKey().ungroup();
    return sortedPt.parallel(new TransformFunc<Pair<T, Void>, T>() {
      @Override
      public void process(Pair<T, Void> input, Saver<T> saver) {
        saver.save(input.key());
      }
    }, collection.getType());
  }


}
