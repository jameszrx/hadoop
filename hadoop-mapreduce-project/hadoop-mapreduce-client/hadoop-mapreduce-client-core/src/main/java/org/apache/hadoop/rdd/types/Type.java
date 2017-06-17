
package org.apache.hadoop.rdd.types;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.io.SourceTarget;

import java.io.Serializable;
import java.util.List;


public interface Type<T> extends Serializable {

  Class<T> getTypeClass();

  MapFunc<Object, T> getInputMapFunc();

  MapFunc<T, Object> getOutputMapFunc();

  Converter getConverter();

  T getDetachedValue(T value);

  SourceTarget<T> getDefaultFileSource(Path path);


  List<Type> getSubTypes();
}
