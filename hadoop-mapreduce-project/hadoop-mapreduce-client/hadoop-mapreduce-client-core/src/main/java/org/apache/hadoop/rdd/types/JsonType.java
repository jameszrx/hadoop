
package org.apache.hadoop.rdd.types;

import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.types.writable.Writables;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;


public class JsonType {

  public static <T> Type<T> jsonString(Class<T> clazz) {
    return Writables
        .derived(clazz, new JsonInputMapFunc<T>(clazz), new JsonOutputMapFunc<T>(), Writables.strings());
  }

  public static class JsonInputMapFunc<T> extends MapFunc<String, T> {

    private final Class<T> clazz;
    private transient ObjectMapper mapper;

    JsonInputMapFunc(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public void init() {
      this.mapper = new ObjectMapper();
    }

    @Override
    public T map(String input) {
      try {
        return mapper.readValue(input, clazz);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  public static class JsonOutputMapFunc<T> extends MapFunc<T, String> {

    private transient ObjectMapper mapper;

    @Override
    public void init() {
      this.mapper = new ObjectMapper();
    }

    @Override
    public String map(T input) {
      try {
        return mapper.writeValueAsString(input);
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }
  }

}
