
package org.apache.hadoop.rdd.job;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.rdd.func.SelfFunc;
import org.apache.hadoop.rdd.io.*;
import org.apache.hadoop.rdd.io.text.TextFileSourceTarget;
import org.apache.hadoop.rdd.plan.Planner;
import org.apache.hadoop.rdd.structure.InputList;
import org.apache.hadoop.rdd.structure.RDDList;
import org.apache.hadoop.rdd.structure.RDDListImpl;
import org.apache.hadoop.rdd.types.Type;
import org.apache.hadoop.rdd.types.writable.Writables;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class Stream {

  private static final Random RANDOM = new Random();

  private final Class<?> jarClass;
  private final String name;
  private final Map<RDDListImpl<?>, Set<Target>> outputTargets;
  private final Map<RDDListImpl<?>, MaterializableIterable<?>> outputTargetsToMaterialize;
  private Path tempDirectory;
  private int tempFileIndex;
  private int nextAnonymousStageId;

  private Configuration conf;


  public Stream(Class<?> jarClass, Configuration conf) {
    this(jarClass, jarClass.getName(), conf);
  }

  public Stream(Class<?> jarClass, String name, Configuration conf) {
    this.jarClass = jarClass;
    this.name = name;
    this.outputTargets = Maps.newHashMap();
    this.outputTargetsToMaterialize = Maps.newHashMap();
    this.conf = conf;
    this.tempDirectory = createTempDirectory(conf);
    this.tempFileIndex = 0;
    this.nextAnonymousStageId = 0;
  }

  public Configuration getConfiguration() {
    return conf;
  }


  public StreamResult run() {
    Planner planner = new Planner(this, outputTargets);
    StreamResult res;
    try {
      res = planner.plan(jarClass, conf).execute();
    } catch (IOException e) {
      e.printStackTrace();
      return StreamResult.EMPTY;
    }
    for (RDDListImpl<?> c : outputTargets.keySet()) {
      if (outputTargetsToMaterialize.containsKey(c)) {
        MaterializableIterable iter = outputTargetsToMaterialize.get(c);
        iter.materialize();
        c.materializeAt(iter.getSourceTarget());
        outputTargetsToMaterialize.remove(c);
      } else {
        boolean materialized = false;
        for (Target t : outputTargets.get(c)) {
          if (!materialized && t instanceof Source) {
            c.materializeAt((SourceTarget) t);
            materialized = true;
          }
        }
      }
    }
    outputTargets.clear();
    return res;
  }

  public StreamResult done() {
    StreamResult res = null;
    if (!outputTargets.isEmpty()) {
      res = run();
    }
    cleanup();
    return res;
  }

  public <S> RDDList<S> read(Source<S> source) {
    return new InputList<S>(source, this);
  }



  public static TextFileSourceTarget<String> textFile(String pathName) {
    return new TextFileSourceTarget<>(new Path(pathName), Writables.strings());
  }


  public RDDList<String> readTextFile(String pathName) {
    return read(textFile(pathName));
  }

  @SuppressWarnings("unchecked")
  public void write(RDDList<?> list, Target target) {
    addOutput((RDDListImpl<?>) list, target);
  }

  private void addOutput(RDDListImpl<?> impl, Target target) {
    if (!outputTargets.containsKey(impl)) {
      outputTargets.put(impl, Sets.newHashSet());
    }
    outputTargets.get(impl).add(target);
  }

  public <T> Iterable<T> materialize(RDDList<T> list) {

    RDDListImpl<T> rddListImpl = toRDDListImpl(list);
    ReadableSourceTarget<T> srcTarget = getMaterializeSourceTarget(rddListImpl);

    MaterializableIterable<T> c = new MaterializableIterable<T>(this, srcTarget);
    if (!outputTargetsToMaterialize.containsKey(rddListImpl)) {
      outputTargetsToMaterialize.put(rddListImpl, c);
    }
    return c;
  }


  public <T> ReadableSourceTarget<T> getMaterializeSourceTarget(RDDList<T> list) {
    RDDListImpl<T> impl = toRDDListImpl(list);
    SourceTarget<T> matTarget = impl.getMaterializedAt();
    if (matTarget != null && matTarget instanceof ReadableSourceTarget) {
      return (ReadableSourceTarget<T>) matTarget;
    }

      SourceTarget<T> st = createTempOutput(list.getType());
      ReadableSourceTarget<T> srcTarget = (ReadableSourceTarget<T>) st;
      addOutput(impl, srcTarget);

    return srcTarget;
  }


  private <T> RDDListImpl<T> toRDDListImpl(RDDList<T> list) {
    return (RDDListImpl<T>) list;
  }

  public <T> SourceTarget<T> createTempOutput(Type<T> ptype) {
    return ptype.getDefaultFileSource(createTempPath());
  }

  public Path createTempPath() {
    tempFileIndex++;
    return new Path(tempDirectory, "rdd" + tempFileIndex);
  }

  private static Path createTempDirectory(Configuration conf) {
    Path dir = createTemporaryPath(conf);
    try {
      dir.getFileSystem(conf).mkdirs(dir);
    } catch (IOException e) {
      throw new RuntimeException("Cannot create job output directory " + dir, e);
    }
    return dir;
  }

  private static Path createTemporaryPath(Configuration conf) {
    String baseDir = conf.get(RuntimeParameters.TMP_DIR, "/tmp");
    return new Path(baseDir, "rdd-" + (RANDOM.nextInt() & Integer.MAX_VALUE));
  }

  public <T> void writeTextFile(RDDList<T> rddList, String pathName) {

    rddList = rddList.parallel("asText", SelfFunc.getInstance(),
            rddList.getType());
    write(rddList, textFile(pathName));
  }

  private void cleanup() {
    try {
      FileSystem fs = tempDirectory.getFileSystem(conf);
      if (fs.exists(tempDirectory)) {
        fs.delete(tempDirectory, true);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public int getNextAnonymousStageId() {
    return nextAnonymousStageId++;
  }



  public String getName() {
    return name;
  }
}
