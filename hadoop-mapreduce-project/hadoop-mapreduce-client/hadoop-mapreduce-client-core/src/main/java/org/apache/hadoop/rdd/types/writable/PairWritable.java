
package org.apache.hadoop.rdd.types.writable;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class PairWritable implements WritableComparable<PairWritable> {

  private long written;
  private Writable[] values;


  public PairWritable() {
  }


  public PairWritable(Writable[] vals) {
    written = 0L;
    values = vals;
  }


  public boolean has(int i) {
    return 0 != ((1 << i) & written);
  }


  public Writable get(int i) {
    return values[i];
  }


  public int size() {
    return values.length;
  }


  public boolean equals(Object other) {
    if (other instanceof PairWritable) {
      PairWritable that = (PairWritable) other;
      if (this.size() != that.size() || this.written != that.written) {
        return false;
      }
      for (int i = 0; i < values.length; ++i) {
        if (!has(i))
          continue;
        if (!values[i].equals(that.get(i))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();
    builder.append(written);
    for (Writable v : values) {
      builder.append(v);
    }
    return builder.toHashCode();
  }


  public String toString() {
    StringBuffer buf = new StringBuffer("[");
    for (int i = 0; i < values.length; ++i) {
      buf.append(has(i) ? values[i].toString() : "");
      buf.append(",");
    }
    if (values.length != 0)
      buf.setCharAt(buf.length() - 1, ']');
    else
      buf.append(']');
    return buf.toString();
  }


  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, values.length);
    WritableUtils.writeVLong(out, written);
    for (int i = 0; i < values.length; ++i) {
      if (has(i)) {
        Text.writeString(out, values[i].getClass().getName());
      }
    }
    for (int i = 0; i < values.length; ++i) {
      if (has(i)) {
        values[i].write(out);
      }
    }
  }


  @SuppressWarnings("unchecked")
  public void readFields(DataInput in) throws IOException {
    int card = WritableUtils.readVInt(in);
    values = new Writable[card];
    written = WritableUtils.readVLong(in);
    Class<? extends Writable>[] cls = new Class[card];
    try {
      for (int i = 0; i < card; ++i) {
        if (has(i)) {
          cls[i] = Class.forName(Text.readString(in)).asSubclass(Writable.class);
        }
      }
      for (int i = 0; i < card; ++i) {
        if (has(i)) {
          values[i] = cls[i].newInstance();
          values[i].readFields(in);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  public void setWritten(int i) {
    written |= 1 << i;
  }

  void clearWritten() {
    written = 0L;
  }

  @Override
  public int compareTo(PairWritable o) {
    for (int i = 0; i < values.length; ++i) {
      if (has(i) && !o.has(i)) {
        return 1;
      } else if (!has(i) && o.has(i)) {
        return -1;
      } else {
        Writable v1 = values[i];
        Writable v2 = o.values[i];
        if (v1 != v2 && (v1 != null && !v1.equals(v2))) {
          if (v1 instanceof WritableComparable && v2 instanceof WritableComparable) {
            int cmp = ((WritableComparable) v1).compareTo(v2);
            if (cmp != 0) {
              return cmp;
            }
          } else {
            int cmp = v1.hashCode() - v2.hashCode();
            if (cmp != 0) {
              return cmp;
            }
          }
        }
      }
    }
    return values.length - o.values.length;
  }
}