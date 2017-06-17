
package org.apache.hadoop.rdd.io.seq;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.rdd.io.Source;
import org.apache.hadoop.rdd.io.impl.ReadableSourcePathTargetImpl;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.types.RDDPairListType;

public class SeqPairListSourceTarget<K, V> extends ReadableSourcePathTargetImpl<Pair<K, V>> implements
        Source<Pair<K, V>> {

  public SeqPairListSourceTarget(Path path, RDDPairListType<K, V> tableType) {
    super(new SeqPairListSource<K, V>(path, tableType), new SeqFileTarget(path));
  }
}
