
package org.apache.hadoop.rdd.job;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.rdd.job.jobcontrol.RDDControlledJob;
import org.apache.hadoop.rdd.plan.OutputHandler;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.List;

public class RDDJob extends RDDControlledJob {


  private Path workPath;
  private List<Path> paths;
  private boolean mapOnly;

  public RDDJob(Job job, Path workPath, OutputHandler handler) throws IOException {
    super(job, Lists.newArrayList());
    this.workPath = workPath;
    this.paths = handler.getPaths();
    this.mapOnly = handler.isMapOnly();
  }

  private synchronized void handlePaths() throws IOException {
    if (!paths.isEmpty()) {
      FileSystem srcFs = workPath.getFileSystem(job.getConfiguration());
      for (int i = 0; i < paths.size(); i++) {
        Path src = new Path(workPath, "out" + i + "-*");
        Path[] srcs = FileUtil.stat2Paths(srcFs.globStatus(src), src);
        Path target = paths.get(i);
        FileSystem targetFs = target.getFileSystem(job.getConfiguration());
        if (!targetFs.exists(target)) {
          targetFs.mkdirs(target);
        }
        int minPartIndex = targetFs.listStatus(target).length;
        for (Path s : srcs) {
          Path d = getTargetFile(target, minPartIndex++);
          srcFs.rename(s, d);
        }
      }
    }
  }


  private Path getTargetFile(Path dir, int idx) {

    String file;
    if(mapOnly)
      file = String.format("part-%s-%d", "m", idx);
    else
      file = String.format("part-%s-%d", "r", idx);
    return new Path(dir, file);
  }


  @Override
  protected void checkRunningState() throws IOException, InterruptedException {
    if (job.isComplete()) {
      if (job.isSuccessful()) {
        handlePaths();
        this.state = State.SUCCESS;
      } else {
        this.state = State.FAILED;
        this.message = "Job failed!";
      }
    }
  }

}
