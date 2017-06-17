
package org.apache.hadoop.rdd.job.jobcontrol;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class RDDControlledJob {


  public enum State {
    SUCCESS, WAITING, RUNNING, READY, FAILED, DEPENDENT_FAILED
  }

  public static final String CREATE_DIR = "mapreduce.jobcontrol.createdir.ifnotexist";
  protected State state;
  protected Job job;

  protected String message;
  private String controlID;

  private List<RDDControlledJob> dependingJobs;


  public RDDControlledJob(Job job, List<RDDControlledJob> dependingJobs)
      throws IOException {
    this.job = job;
    this.dependingJobs = dependingJobs;
    this.state = State.WAITING;
    this.controlID = "unassigned";
    this.message = "just initialized";
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("job name:\t").append(this.job.getJobName()).append("\n");
    sb.append("job id:\t").append(this.controlID).append("\n");
    sb.append("job state:\t").append(this.state).append("\n");
    sb.append("job mapred id:\t").append(this.job.getJobID()).append("\n");
    sb.append("job message:\t").append(this.message).append("\n");

    if (this.dependingJobs == null || this.dependingJobs.size() == 0) {
      sb.append("job has no depending job:\t").append("\n");
    } else {
      sb.append("job has ").append(this.dependingJobs.size())
          .append(" dependeng jobs:\n");
      for (int i = 0; i < this.dependingJobs.size(); i++) {
        sb.append("\t depending job ").append(i).append(":\t");
        sb.append((this.dependingJobs.get(i)).getJobName()).append("\n");
      }
    }
    return sb.toString();
  }


  public String getJobName() {
    return job.getJobName();
  }


  public void setJobName(String jobName) {
    job.setJobName(jobName);
  }


  public String getJobID() {
    return this.controlID;
  }


  public void setJobID(String id) {
    this.controlID = id;
  }







  public synchronized Job getJob() {
    return this.job;
  }


  public synchronized void setJob(Job job) {
    this.job = job;
  }


  public synchronized State getJobState() {
    return this.state;
  }


  protected synchronized void setJobState(State state) {
    this.state = state;
  }


  public synchronized String getMessage() {
    return this.message;
  }


  public synchronized void setMessage(String message) {
    this.message = message;
  }


  public List<RDDControlledJob> getDependentJobs() {
    return this.dependingJobs;
  }


  public synchronized boolean addDependingJob(RDDControlledJob dependingJob) {
    if (this.state == State.WAITING) {
      if (this.dependingJobs == null) {
        this.dependingJobs = new ArrayList<RDDControlledJob>();
      }
      return this.dependingJobs.add(dependingJob);
    } else {
      return false;
    }
  }


  public synchronized boolean isCompleted() {
    return this.state == State.FAILED || this.state == State.DEPENDENT_FAILED
        || this.state == State.SUCCESS;
  }


  public synchronized boolean isReady() {
    return this.state == State.READY;
  }

  public void killJob() throws IOException, InterruptedException {
    job.killJob();
  }


  protected void checkRunningState() throws IOException, InterruptedException {
    try {
      if (job.isComplete()) {
        if (job.isSuccessful()) {
          this.state = State.SUCCESS;
        } else {
          this.state = State.FAILED;
          this.message = "Job failed!";
        }
      }
    } catch (IOException ioe) {
      this.state = State.FAILED;
      this.message = StringUtils.stringifyException(ioe);
      try {
        if (job != null) {
          job.killJob();
        }
      } catch (IOException e) {
      }
    }
  }


  synchronized State checkState() throws IOException, InterruptedException {
    if (this.state == State.RUNNING) {
      checkRunningState();
    }
    if (this.state != State.WAITING) {
      return this.state;
    }
    if (this.dependingJobs == null || this.dependingJobs.size() == 0) {
      this.state = State.READY;
      return this.state;
    }
    RDDControlledJob pred = null;
    int n = this.dependingJobs.size();
    for (int i = 0; i < n; i++) {
      pred = this.dependingJobs.get(i);
      State s = pred.checkState();
      if (s == State.WAITING || s == State.READY || s == State.RUNNING) {
        break;

      }
      if (s == State.FAILED || s == State.DEPENDENT_FAILED) {
        this.state = State.DEPENDENT_FAILED;
        this.message = "depending job " + i + " with jobID " + pred.getJobID()
            + " failed. " + pred.getMessage();
        break;
      }

      if (i == n - 1) {
        this.state = State.READY;
      }
    }

    return this.state;
  }


  protected synchronized void submit() {
    try {
      Configuration conf = job.getConfiguration();
      if (conf.getBoolean(CREATE_DIR, false)) {
        Path[] inputPaths = FileInputFormat.getInputPaths(job);
        for (Path inputPath : inputPaths) {
          FileSystem fs = inputPath.getFileSystem(conf);
          if (!fs.exists(inputPath)) {
              fs.mkdirs(inputPath);
          }
        }
      }
      job.submit();
      this.state = State.RUNNING;
    } catch (Exception ioe) {
      this.state = State.FAILED;
      this.message = StringUtils.stringifyException(ioe);
    }
  }

}
