
package org.apache.hadoop.rdd.job;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.rdd.job.jobcontrol.RDDControlledJob;
import org.apache.hadoop.rdd.job.jobcontrol.RDDJobControl;

import java.util.List;


public class Executor {

  private static final Log LOG = LogFactory.getLog(Executor.class);

  private final RDDJobControl control;

  public Executor(Class<?> jarClass) {
    this.control = new RDDJobControl(jarClass.toString());
  }

  public void addJob(RDDJob job) {
    this.control.addJob(job);
  }

  public StreamResult execute() {
    try {
      Thread controlThread = new Thread(control);
      controlThread.start();
      while (!control.allFinished()) {
        Thread.sleep(1000);
      }
      control.stop();
    } catch (InterruptedException e) {
      LOG.info(e);
    }
    List<RDDControlledJob> failures = control.getFailedJobList();
    if (!failures.isEmpty()) {
      LOG.error(failures.size() + " job failure(s) occurred:");
      for (RDDControlledJob job : failures) {
        LOG.error(job.getJobName() + "(" + job.getJobID() + "): " + job.getMessage());
      }
    }
    List<StreamResult.StageResult> stages = Lists.newArrayList();
    for (RDDControlledJob job : control.getSuccessfulJobList()) {
      try {
        stages.add(new StreamResult.StageResult(job.getJob().getCounters()));
      } catch (Exception e) {
        LOG.error("Exception thrown fetching job counters for stage: " + job.getJobName(), e);
      }
    }
    return new StreamResult(stages);
  }
}
