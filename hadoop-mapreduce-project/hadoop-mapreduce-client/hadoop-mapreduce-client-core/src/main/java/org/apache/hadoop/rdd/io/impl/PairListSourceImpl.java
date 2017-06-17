
package org.apache.hadoop.rdd.io.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.rdd.io.Source;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.types.RDDPairListType;

public class PairListSourceImpl<K, V> extends FileSourceImpl<Pair<K, V>> implements Source<Pair<K, V>> {

  public PairListSourceImpl(Path path, RDDPairListType<K, V> tableType, Class<? extends FileInputFormat> formatClass) {
    super(path, tableType, formatClass);
  }





}
