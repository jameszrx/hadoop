
package org.apache.hadoop.rdd.types.writable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.rdd.func.CompositeMapFunc;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.func.SelfFunc;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.types.PairFactory;
import org.apache.hadoop.rdd.types.Type;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;


public class Writables {
  private static final MapFunc<NullWritable, Void> NULL_WRITABLE_TO_VOID = new MapFunc<NullWritable, Void>() {
    @Override
    public Void map(NullWritable input) {
      return null;
    }
  };

  private static final MapFunc<Void, NullWritable> VOID_TO_NULL_WRITABLE = new MapFunc<Void, NullWritable>() {
    @Override
    public NullWritable map(Void input) {
      return NullWritable.get();
    }
  };

  private static final MapFunc<Text, String> TEXT_TO_STRING = new MapFunc<Text, String>() {
    @Override
    public String map(Text input) {
      return input.toString();
    }
  };

  private static final MapFunc<String, Text> STRING_TO_TEXT = new MapFunc<String, Text>() {
    @Override
    public Text map(String input) {
      return new Text(input);
    }
  };

  private static final MapFunc<IntWritable, Integer> IW_TO_INT = new MapFunc<IntWritable, Integer>() {
    @Override
    public Integer map(IntWritable input) {
      return input.get();
    }
  };

  private static final MapFunc<Integer, IntWritable> INT_TO_IW = new MapFunc<Integer, IntWritable>() {
    @Override
    public IntWritable map(Integer input) {
      return new IntWritable(input);
    }
  };

  private static final MapFunc<LongWritable, Long> LW_TO_LONG = new MapFunc<LongWritable, Long>() {
    @Override
    public Long map(LongWritable input) {
      return input.get();
    }
  };

  private static final MapFunc<Long, LongWritable> LONG_TO_LW = new MapFunc<Long, LongWritable>() {
    @Override
    public LongWritable map(Long input) {
      return new LongWritable(input);
    }
  };

  private static final MapFunc<FloatWritable, Float> FW_TO_FLOAT = new MapFunc<FloatWritable, Float>() {
    @Override
    public Float map(FloatWritable input) {
      return input.get();
    }
  };

  private static final MapFunc<Float, FloatWritable> FLOAT_TO_FW = new MapFunc<Float, FloatWritable>() {
    @Override
    public FloatWritable map(Float input) {
      return new FloatWritable(input);
    }
  };

  private static final MapFunc<DoubleWritable, Double> DW_TO_DOUBLE = new MapFunc<DoubleWritable, Double>() {
    @Override
    public Double map(DoubleWritable input) {
      return input.get();
    }
  };

  private static final MapFunc<Double, DoubleWritable> DOUBLE_TO_DW = new MapFunc<Double, DoubleWritable>() {
    @Override
    public DoubleWritable map(Double input) {
      return new DoubleWritable(input);
    }
  };

  private static final MapFunc<BooleanWritable, Boolean> BW_TO_BOOLEAN = new MapFunc<BooleanWritable, Boolean>() {
    @Override
    public Boolean map(BooleanWritable input) {
      return input.get();
    }
  };

  private static final BooleanWritable TRUE = new BooleanWritable(true);
  private static final BooleanWritable FALSE = new BooleanWritable(false);
  private static final MapFunc<Boolean, BooleanWritable> BOOLEAN_TO_BW = new MapFunc<Boolean, BooleanWritable>() {
    @Override
    public BooleanWritable map(Boolean input) {
      return input == Boolean.TRUE ? TRUE : FALSE;
    }
  };

  private static final MapFunc<BytesWritable, ByteBuffer> BW_TO_BB = new MapFunc<BytesWritable, ByteBuffer>() {
    @Override
    public ByteBuffer map(BytesWritable input) {
      return ByteBuffer.wrap(input.getBytes(), 0, input.getLength());
    }
  };

  private static final MapFunc<ByteBuffer, BytesWritable> BB_TO_BW = new MapFunc<ByteBuffer, BytesWritable>() {
    @Override
    public BytesWritable map(ByteBuffer input) {
      BytesWritable bw = new BytesWritable();
      bw.set(input.array(), input.arrayOffset(), input.limit());
      return bw;
    }
  };

  private static <S, W extends Writable> WritableType<S, W> create(Class<S> typeClass, Class<W> writableClass,
                                                                   MapFunc<W, S> inputFunc, MapFunc<S, W> outputFunc) {
    return new WritableType<>(typeClass, writableClass, inputFunc, outputFunc);
  }

  private static final WritableType<Void, NullWritable> nulls = create(Void.class, NullWritable.class,
      NULL_WRITABLE_TO_VOID, VOID_TO_NULL_WRITABLE);
  private static final WritableType<String, Text> strings = create(String.class, Text.class, TEXT_TO_STRING,
      STRING_TO_TEXT);
  private static final WritableType<Long, LongWritable> longs = create(Long.class, LongWritable.class, LW_TO_LONG,
      LONG_TO_LW);
  private static final WritableType<Integer, IntWritable> ints = create(Integer.class, IntWritable.class, IW_TO_INT,
      INT_TO_IW);
  private static final WritableType<Float, FloatWritable> floats = create(Float.class, FloatWritable.class,
      FW_TO_FLOAT, FLOAT_TO_FW);
  private static final WritableType<Double, DoubleWritable> doubles = create(Double.class, DoubleWritable.class,
      DW_TO_DOUBLE, DOUBLE_TO_DW);
  private static final WritableType<Boolean, BooleanWritable> booleans = create(Boolean.class, BooleanWritable.class,
      BW_TO_BOOLEAN, BOOLEAN_TO_BW);
  private static final WritableType<ByteBuffer, BytesWritable> bytes = create(ByteBuffer.class, BytesWritable.class,
      BW_TO_BB, BB_TO_BW);

  private static final Map<Class<?>, WritableType<?, ?>> EXTENSIONS = Maps.newHashMap();


  public static <T> void register(Class<T> clazz, WritableType<T, ? extends Writable> ptype) {
    EXTENSIONS.put(clazz, ptype);
  }

  public static final WritableType<Void, NullWritable> nulls() {
    return nulls;
  }

  public static final WritableType<String, Text> strings() {
    return strings;
  }

  public static final WritableType<Long, LongWritable> longs() {
    return longs;
  }

  public static final WritableType<Integer, IntWritable> ints() {
    return ints;
  }

  public static final WritableType<Float, FloatWritable> floats() {
    return floats;
  }

  public static final WritableType<Double, DoubleWritable> doubles() {
    return doubles;
  }

  public static final WritableType<Boolean, BooleanWritable> booleans() {
    return booleans;
  }

  public static final WritableType<ByteBuffer, BytesWritable> bytes() {
    return bytes;
  }

  public static final <T, W extends Writable> WritableType<T, W> records(Class<T> clazz) {
    if (EXTENSIONS.containsKey(clazz)) {
      return (WritableType<T, W>) EXTENSIONS.get(clazz);
    }
    return (WritableType<T, W>) writables(clazz.asSubclass(Writable.class));
  }

  public static <W extends Writable> WritableType<W, W> writables(Class<W> clazz) {
    MapFunc wIdentity = SelfFunc.getInstance();
    return new WritableType<W, W>(clazz, clazz, wIdentity, wIdentity);
  }

  public static <K, V> WritablePairListType<K, V> tableOf(Type<K> key, Type<V> value) {
    if (key instanceof WritablePairListType) {
      WritablePairListType wtt = (WritablePairListType) key;
      key = pairs(wtt.getKeyType(), wtt.getValueType());
    } else if (!(key instanceof WritableType)) {
      throw new IllegalArgumentException("Key type must be of class WritableType");
    }
    if (value instanceof WritablePairListType) {
      WritablePairListType wtt = (WritablePairListType) value;
      value = pairs(wtt.getKeyType(), wtt.getValueType());
    } else if (!(value instanceof WritableType)) {
      throw new IllegalArgumentException("Value type must be of class WritableType");
    }
    return new WritablePairListType((WritableType) key, (WritableType) value);
  }


  private static class InputPairMapFunc extends MapFunc<PairWritable, Pair> {
    private final PairFactory pairFactory;
    private final List<MapFunc> funcs;

    private transient Object[] values;

    public InputPairMapFunc(PairFactory pairFactory, Type<?>... ptypes) {
      this.pairFactory = pairFactory;
      this.funcs = Lists.newArrayList();
      for (Type ptype : ptypes) {
        funcs.add(ptype.getInputMapFunc());
      }
    }

    @Override
    public void configure(Configuration conf) {
      for (MapFunc f : funcs) {
        f.configure(conf);
      }
    }


    @Override
    public void init() {
      for (MapFunc f : funcs) {
        f.setContext(getContext());
      }



      this.values = new Object[funcs.size()];
      pairFactory.initialize();
    }

    @Override
    public Pair map(PairWritable in) {
      for (int i = 0; i < values.length; i++) {
        if (in.has(i)) {
          values[i] = funcs.get(i).map(in.get(i));
        } else {
          values[i] = null;
        }
      }
      return pairFactory.makePair(values);
    }
  }


  private static class outputPairMapFunc extends MapFunc<Pair, PairWritable> {

    private transient PairWritable writable;
    private transient Writable[] values;

    private final List<MapFunc> funcs;

    public outputPairMapFunc(Type<?>... ptypes) {
      this.funcs = Lists.newArrayList();
      for (Type<?> ptype : ptypes) {
        funcs.add(ptype.getOutputMapFunc());
      }
    }

    @Override
    public void configure(Configuration conf) {
      for (MapFunc f : funcs) {
        f.configure(conf);
      }
    }



    @Override
    public void init() {
      this.values = new Writable[funcs.size()];
      this.writable = new PairWritable(values);
      for (MapFunc f : funcs) {
        f.setContext(getContext());
      }
    }

    @Override
    public PairWritable map(Pair input) {
      writable.clearWritten();
      for (int i = 0; i < input.size(); i++) {
        Object value = input.get(i);
        if (value != null) {
          writable.setWritten(i);
          values[i] = (Writable) funcs.get(i).map(value);
        }
      }
      return writable;
    }
  }

  public static <V1, V2> WritableType<Pair<V1, V2>, PairWritable> pairs(Type<V1> p1, Type<V2> p2) {
    InputPairMapFunc input = new InputPairMapFunc(PairFactory.PAIR, p1, p2);
    outputPairMapFunc output = new outputPairMapFunc(p1, p2);
    return new WritableType(Pair.class, PairWritable.class, input, output, p1, p2);
  }




  public static <S, T> Type<T> derived(Class<T> clazz, MapFunc<S, T> inputFunc, MapFunc<T, S> outputFunc, Type<S> base) {
    WritableType<S, ?> wt = (WritableType<S, ?>) base;
    MapFunc input = new CompositeMapFunc(wt.getInputMapFunc(), inputFunc);
    MapFunc output = new CompositeMapFunc(outputFunc, wt.getOutputMapFunc());
    return new WritableType(clazz, wt.getSerializationClass(), input, output, base.getSubTypes().toArray(new Type[0]));
  }

  private static class ArrayCollectionMapFunc<T> extends MapFunc<GenericArrayWritable, Collection<T>> {
    private final MapFunc<Object, T> mapFunc;

    public ArrayCollectionMapFunc(MapFunc<Object, T> mapFunc) {
      this.mapFunc = mapFunc;
    }

    @Override
    public void configure(Configuration conf) {
      mapFunc.configure(conf);
    }






    @Override
    public void init() {
      mapFunc.setContext(getContext());
    }

    @Override
    public Collection<T> map(GenericArrayWritable input) {
      Collection<T> collection = Lists.newArrayList();
      for (Writable writable : input.get()) {
        collection.add(mapFunc.map(writable));
      }
      return collection;
    }
  }

  private static class CollectionArrayMapFunc<T> extends MapFunc<Collection<T>, GenericArrayWritable> {

    private final Class<? extends Writable> clazz;
    private final MapFunc<T, Object> mapFunc;

    public CollectionArrayMapFunc(Class<? extends Writable> clazz, MapFunc<T, Object> mapFunc) {
      this.clazz = clazz;
      this.mapFunc = mapFunc;
    }

    @Override
    public void configure(Configuration conf) {
      mapFunc.configure(conf);
    }


    @Override
    public void init() {
      mapFunc.setContext(getContext());
    }

    @Override
    public GenericArrayWritable map(Collection<T> input) {
      GenericArrayWritable arrayWritable = new GenericArrayWritable(clazz);
      Writable[] w = new Writable[input.size()];
      int index = 0;
      for (T in : input) {
        w[index++] = ((Writable) mapFunc.map(in));
      }
      arrayWritable.set(w);
      return arrayWritable;
    }
  }

  public static <T> WritableType<Collection<T>, GenericArrayWritable<T>> collections(Type<T> ptype) {
    WritableType<T, ?> wt = (WritableType<T, ?>) ptype;
    return new WritableType(Collection.class, GenericArrayWritable.class, new ArrayCollectionMapFunc(wt.getInputMapFunc()),
        new CollectionArrayMapFunc(wt.getSerializationClass(), wt.getOutputMapFunc()), ptype);
  }


  private Writables() {
  }
}
