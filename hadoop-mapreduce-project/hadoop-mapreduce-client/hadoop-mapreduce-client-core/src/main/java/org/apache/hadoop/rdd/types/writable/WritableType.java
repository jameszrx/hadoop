
package org.apache.hadoop.rdd.types.writable;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.io.SourceTarget;
import org.apache.hadoop.rdd.io.seq.SeqFileSourceTarget;
import org.apache.hadoop.rdd.types.Converter;
import org.apache.hadoop.rdd.types.Type;

import java.util.List;

public class WritableType<T, W extends Writable> implements Type<T> {

  private final Class<T> typeClass;
  private final Class<W> writableClass;
  private final Converter converter;
  private final MapFunc<W, T> inputFunc;
  private final MapFunc<T, W> outputFunc;

  private final List<Type> subTypes;

  WritableType(Class<T> typeClass, Class<W> writableClass, MapFunc<W, T> inputDoFunc, MapFunc<T, W> outputDoFunc,
               Type... subTypes) {
    this.typeClass = typeClass;
    this.writableClass = writableClass;
    this.inputFunc = inputDoFunc;
    this.outputFunc = outputDoFunc;
    this.converter = new WritableValueConverter(writableClass);

    this.subTypes = ImmutableList.<Type> builder().add(subTypes).build();
  }






  @Override
  public Class<T> getTypeClass() {
    return typeClass;
  }

  @Override
  public Converter getConverter() {
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
  public List<Type> getSubTypes() {
    return subTypes;
  }

  public Class<W> getSerializationClass() {
    return writableClass;
  }

  @Override
  public SourceTarget<T> getDefaultFileSource(Path path) {
    return new SeqFileSourceTarget<T>(path, this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof WritableType)) {
      return false;
    }
    WritableType wt = (WritableType) obj;
    return (typeClass.equals(wt.typeClass) && writableClass.equals(wt.writableClass) && subTypes.equals(wt.subTypes));
  }

  @Override
  public T getDetachedValue(T value) {
    W writableValue = outputFunc.map(value);

    return inputFunc.map(writableValue);
  }

//  @Override
//  public int hashCode() {
//    HashCodeBuilder hcb = new HashCodeBuilder();
//    hcb.append(typeClass).append(writableClass).append(subTypes);
//    return hcb.toHashCode();
//  }
}