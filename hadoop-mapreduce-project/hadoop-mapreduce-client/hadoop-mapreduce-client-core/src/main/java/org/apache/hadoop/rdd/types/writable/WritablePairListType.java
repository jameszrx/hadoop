
package org.apache.hadoop.rdd.types.writable;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.func.PairMapFunc;
import org.apache.hadoop.rdd.io.SourceTarget;
import org.apache.hadoop.rdd.io.seq.SeqPairListSourceTarget;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.transformation.RDDPairLists;
import org.apache.hadoop.rdd.types.Converter;
import org.apache.hadoop.rdd.types.GroupedPairListType;
import org.apache.hadoop.rdd.types.RDDPairListType;
import org.apache.hadoop.rdd.types.Type;

import java.util.List;

class WritablePairListType<K, V> implements RDDPairListType<K, V> {

  private final WritableType<K, Writable> keyType;
  private final WritableType<V, Writable> valueType;
  private final MapFunc inputFunc;
  private final MapFunc outputFunc;
  private final Converter converter;

  public WritablePairListType(WritableType<K, Writable> keyType, WritableType<V, Writable> valueType) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.inputFunc = new PairMapFunc(keyType.getInputMapFunc(), valueType.getInputMapFunc());
    this.outputFunc = new PairMapFunc(keyType.getOutputMapFunc(), valueType.getOutputMapFunc());
    this.converter = new WritablePairConverter(keyType.getSerializationClass(), valueType.getSerializationClass());
  }

  @Override
  public Class<Pair<K, V>> getTypeClass() {
    return (Class<Pair<K, V>>) new Pair<>(null, null).getClass();
  }

  @Override
  public List<Type> getSubTypes() {
    return ImmutableList.of(keyType, valueType);
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
  public Converter getConverter() {
    return converter;
  }






  public Type<K> getKeyType() {
    return keyType;
  }

  public Type<V> getValueType() {
    return valueType;
  }

  @Override
  public GroupedPairListType<K, V> getGroupedPairListType() {
    return new WritableGroupedPairListType<K, V>(this);
  }

  @Override
  public SourceTarget<Pair<K, V>> getDefaultFileSource(Path path) {
    return new SeqPairListSourceTarget<K, V>(path, this);
  }

  @Override
  public Pair<K, V> getDetachedValue(Pair<K, V> value) {
    return RDDPairLists.getDetachedValue(this, value);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof WritablePairListType)) {
      return false;
    }
    WritablePairListType that = (WritablePairListType) obj;
    return keyType.equals(that.keyType) && valueType.equals(that.valueType);
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    return hcb.append(keyType).append(valueType).toHashCode();
  }
}