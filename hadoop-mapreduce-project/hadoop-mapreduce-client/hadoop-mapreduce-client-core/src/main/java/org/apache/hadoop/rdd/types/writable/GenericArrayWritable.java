
package org.apache.hadoop.rdd.types.writable;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class GenericArrayWritable<T> implements Writable {
  private Writable[] values;
  private Class<? extends Writable> valueClass;

  public GenericArrayWritable(Class<? extends Writable> valueClass) {
    this.valueClass = valueClass;
  }


  public void set(Writable[] values) {
    this.values = values;
  }

  public Writable[] get() {
    return values;
  }

  public void readFields(DataInput in) throws IOException {
    values = new Writable[WritableUtils.readVInt(in)];
    if (values.length > 0) {
      int nulls = WritableUtils.readVInt(in);
      if (nulls == values.length) {
        return;
      }
      String valueType = Text.readString(in);
      setValueType(valueType);
      for (int i = 0; i < values.length; i++) {
        Writable value = WritableFactories.newInstance(valueClass);
        value.readFields(in);
        values[i] = value;
      }
    }
  }

  protected void setValueType(String valueType) {
    if (valueClass == null) {
      try {
        valueClass = Class.forName(valueType).asSubclass(Writable.class);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    } else if (!valueType.equals(valueClass.getName())) {
      throw new IllegalStateException("Incoming " + valueType + " is not " + valueClass);
    }
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, values.length);
    int nulls = 0;
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        nulls++;
      }
    }
    WritableUtils.writeVInt(out, nulls);
    if (values.length - nulls > 0) {
      if (valueClass == null) {
        throw new IllegalStateException("Value class not set by constructor or read");
      }
      Text.writeString(out, valueClass.getName());
      for (int i = 0; i < values.length; i++) {
        if (values[i] != null) {
          values[i].write(out);
        }
      }
    }
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    return hcb.append(values).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    GenericArrayWritable other = (GenericArrayWritable) obj;
      return Arrays.equals(values, other.values);
  }

  @Override
  public String toString() {
    return Arrays.toString(values);
  }
}
