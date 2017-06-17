
package org.apache.hadoop.rdd.job.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.rdd.job.ContextFactory;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.*;


public class RDDMultipleOutputs<KEYOUT, VALUEOUT> {

  private static final String MULTIPLE_OUTPUTS = "mapreduce.multipleoutputs";

  private static final String MO_PREFIX =
    "mapreduce.multipleoutputs.namedOutput.";

  private static final String PART = "part";
  private static final String FORMAT = ".format";
  private static final String KEY = ".key";
  private static final String VALUE = ".value";
  private static final String COUNTERS_ENABLED =
    "mapreduce.multipleoutputs.counters";

  private static final String BASE_OUTPUT_NAME = "mapreduce.output.basename";


  private static final String COUNTERS_GROUP = RDDMultipleOutputs.class.getName();


  private Map<String, TaskAttemptContext> taskContexts = new HashMap<String, TaskAttemptContext>();


  private static void checkTokenName(String namedOutput) {
    if (namedOutput == null || namedOutput.length() == 0) {
      throw new IllegalArgumentException(
        "Name cannot be NULL or emtpy");
    }
    for (char ch : namedOutput.toCharArray()) {
      if ((ch >= 'A') && (ch <= 'Z')) {
        continue;
      }
      if ((ch >= 'a') && (ch <= 'z')) {
        continue;
      }
      if ((ch >= '0') && (ch <= '9')) {
        continue;
      }
      throw new IllegalArgumentException(
        "Name cannot be have a '" + ch + "' char");
    }
  }


  private static void checkBaseOutputPath(String outputPath) {
    if (outputPath.equals(PART)) {
      throw new IllegalArgumentException("output name cannot be 'part'");
    }
  }


  private static void checkNamedOutputName(JobContext job,
      String namedOutput, boolean alreadyDefined) {
    checkTokenName(namedOutput);
    checkBaseOutputPath(namedOutput);
    List<String> definedChannels = getNamedOutputsList(job);
    if (alreadyDefined && definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput +
        "' already alreadyDefined");
    } else if (!alreadyDefined && !definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput +
        "' not defined");
    }
  }


  private static List<String> getNamedOutputsList(JobContext job) {
    List<String> names = new ArrayList<String>();
    StringTokenizer st = new StringTokenizer(
      job.getConfiguration().get(MULTIPLE_OUTPUTS, ""), " ");
    while (st.hasMoreTokens()) {
      names.add(st.nextToken());
    }
    return names;
  }


  @SuppressWarnings("unchecked")
  private static Class<? extends OutputFormat<?, ?>> getNamedOutputFormatClass(
          JobContext job, String namedOutput) {
    return (Class<? extends OutputFormat<?, ?>>)
      job.getConfiguration().getClass(MO_PREFIX + namedOutput + FORMAT, null,
      OutputFormat.class);
  }


  private static Class<?> getNamedOutputKeyClass(JobContext job,
                                                String namedOutput) {
    return job.getConfiguration().getClass(MO_PREFIX + namedOutput + KEY, null,
      Object.class);
  }


  private static Class<?> getNamedOutputValueClass(
          JobContext job, String namedOutput) {
    return job.getConfiguration().getClass(MO_PREFIX + namedOutput + VALUE,
      null, Object.class);
  }


  public static void addNamedOutput(Job job, String namedOutput,
                                    Class<? extends OutputFormat> outputFormatClass,
                                    Class<?> keyClass, Class<?> valueClass) {
    checkNamedOutputName(job, namedOutput, true);
    Configuration conf = job.getConfiguration();
    conf.set(MULTIPLE_OUTPUTS,
      conf.get(MULTIPLE_OUTPUTS, "") + " " + namedOutput);
    conf.setClass(MO_PREFIX + namedOutput + FORMAT, outputFormatClass,
      OutputFormat.class);
    conf.setClass(MO_PREFIX + namedOutput + KEY, keyClass, Object.class);
    conf.setClass(MO_PREFIX + namedOutput + VALUE, valueClass, Object.class);
  }


  public static boolean getCountersEnabled(JobContext job) {
    return job.getConfiguration().getBoolean(COUNTERS_ENABLED, false);
  }


  @SuppressWarnings("unchecked")
  private static class RecordWriterWithCounter extends RecordWriter {
    private RecordWriter writer;
    private String counterName;
    private TaskInputOutputContext context;

    public RecordWriterWithCounter(RecordWriter writer, String counterName,
                                   TaskInputOutputContext context) {
      this.writer = writer;
      this.counterName = counterName;
      this.context = context;
    }

    @SuppressWarnings({"unchecked"})
    public void write(Object key, Object value)
        throws IOException, InterruptedException {
      context.getCounter(COUNTERS_GROUP, counterName).increment(1);
      writer.write(key, value);
    }

    public void close(TaskAttemptContext context)
        throws IOException, InterruptedException {
      writer.close(context);
    }
  }



  private TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context;
  private Set<String> namedOutputs;
  private Map<String, RecordWriter<?, ?>> recordWriters;
  private boolean countersEnabled;


  public RDDMultipleOutputs(
      TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context) {
    this.context = context;
    namedOutputs = Collections.unmodifiableSet(
      new HashSet<>(RDDMultipleOutputs.getNamedOutputsList(context)));
    recordWriters = new HashMap<>();
    countersEnabled = getCountersEnabled(context);
  }


  @SuppressWarnings("unchecked")
  public <K, V> void write(String namedOutput, K key, V value)
      throws IOException, InterruptedException {
    write(namedOutput, key, value, namedOutput);
  }


  @SuppressWarnings("unchecked")
  public <K, V> void write(String namedOutput, K key, V value,
      String baseOutputPath) throws IOException, InterruptedException {
    checkNamedOutputName(context, namedOutput, false);
    checkBaseOutputPath(baseOutputPath);
    if (!namedOutputs.contains(namedOutput)) {
      throw new IllegalArgumentException("Undefined named output '" +
        namedOutput + "'");
    }
    TaskAttemptContext taskContext = getContext(namedOutput);
    getRecordWriter(taskContext, baseOutputPath).write(key, value);
  }


  @SuppressWarnings("unchecked")
  public void write(KEYOUT key, VALUEOUT value, String baseOutputPath)
      throws IOException, InterruptedException {
    checkBaseOutputPath(baseOutputPath);
    TaskAttemptContext taskContext = ContextFactory.create(
      context.getConfiguration(), context.getTaskAttemptID());
    getRecordWriter(taskContext, baseOutputPath).write(key, value);
  }



  @SuppressWarnings("unchecked")
  private synchronized RecordWriter getRecordWriter(
          TaskAttemptContext taskContext, String baseFileName)
      throws IOException, InterruptedException {

    RecordWriter writer = recordWriters.get(baseFileName);
    if (writer == null) {
      taskContext.getConfiguration().set(BASE_OUTPUT_NAME, baseFileName);
      try {
        writer = ReflectionUtils.newInstance(
          taskContext.getOutputFormatClass(), taskContext.getConfiguration())
          .getRecordWriter(taskContext);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
      if (countersEnabled) {
        writer = new RecordWriterWithCounter(writer, baseFileName, context);
      }
      recordWriters.put(baseFileName, writer);
    }
    return writer;
  }



  private TaskAttemptContext getContext(String nameOutput) throws IOException {

    TaskAttemptContext taskContext = taskContexts.get(nameOutput);

    if (taskContext != null) {
      return taskContext;
    }
    Job job = new Job(context.getConfiguration());
    job.getConfiguration().set("rdd.namedoutput", nameOutput);
    job.setOutputFormatClass(getNamedOutputFormatClass(context, nameOutput));
    job.setOutputKeyClass(getNamedOutputKeyClass(context, nameOutput));
    job.setOutputValueClass(getNamedOutputValueClass(context, nameOutput));
    taskContext = ContextFactory.create(
      job.getConfiguration(), context.getTaskAttemptID());

    taskContexts.put(nameOutput, taskContext);

    return taskContext;
  }


  @SuppressWarnings("unchecked")
  public void close() throws IOException, InterruptedException {
    for (RecordWriter writer : recordWriters.values()) {
      writer.close(context);
    }
  }
}
