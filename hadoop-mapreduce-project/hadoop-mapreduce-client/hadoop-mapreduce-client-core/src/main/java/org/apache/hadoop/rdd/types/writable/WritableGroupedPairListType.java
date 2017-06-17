
package org.apache.hadoop.rdd.types.writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.transformation.RDDPairLists;
import org.apache.hadoop.rdd.types.Converter;
import org.apache.hadoop.rdd.types.GroupedPairListType;

public class WritableGroupedPairListType<K, V> extends GroupedPairListType<K, V> {

  private final MapFunc inputFunc;
  private final MapFunc outputFunc;
  private final Converter converter;

  public WritableGroupedPairListType(WritablePairListType<K, V> tableType) {
    super(tableType);
    WritableType keyType = (WritableType) tableType.getKeyType();
    WritableType valueType = (WritableType) tableType.getValueType();
    this.inputFunc = new PairIterableMapFunc(keyType.getInputMapFunc(), valueType.getInputMapFunc());
    this.outputFunc = tableType.getOutputMapFunc();
    this.converter = new WritablePairConverter(keyType.getSerializationClass(), valueType.getSerializationClass());
  }

  @Override
  public Class<Pair<K, Iterable<V>>> getTypeClass() {
    return (Class<Pair<K, Iterable<V>>>) new Pair(null, null).getClass();
  }

  @Override
  public Converter getGroupingConverter() {
    return converter;
  }

  @Override
  public MapFunc getInputMapFunc() {
    return inputFunc;
  }

  @Override
  public MapFunc getOutputMapFunc() {
    return outputFunc;
  }

  @Override
  public Pair<K, Iterable<V>> getDetachedValue(Pair<K, Iterable<V>> value) {
    return RDDPairLists.getGroupedDetachedValue(this, value);
  }

  @Override
  public void configureShuffle(Job job) {

    WritableType keyType = (WritableType) tableType.getKeyType();
    WritableType valueType = (WritableType) tableType.getValueType();
    job.setMapOutputKeyClass(keyType.getSerializationClass());
    job.setMapOutputValueClass(valueType.getSerializationClass());
  }
}
