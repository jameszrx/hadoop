
package org.apache.hadoop.rdd.job;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

import java.util.List;


public class StreamResult {

  public static class StageResult {

    private final Counters counters;

    public StageResult(Counters counters) {

      this.counters = counters;
    }

    public Counters getCounters() {
      return counters;
    }

    public Counter findCounter(Enum<?> key) {
      return counters.findCounter(key);
    }

  }

  public static final StreamResult EMPTY = new StreamResult(ImmutableList.of());

  private final List<StageResult> stageResults;

  public StreamResult(List<StageResult> stageResults) {
    this.stageResults = ImmutableList.copyOf(stageResults);
  }

  public boolean succeeded() {
    return !stageResults.isEmpty();
  }




}
