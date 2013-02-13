/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Dummy class for testing MR framework. Sleeps for a defined period
 * of time in mapper and reducer. Generates fake input for map / reduce
 * jobs. Note that generated number of input pairs is in the order
 * of <code>numMappers * mapSleepTime / 100</code>, so the job uses
 * some disk space.
 */
public class SleepJob extends Configured implements Tool,
             Mapper<IntWritable, IntWritable, IntWritable, NullWritable>,
             Reducer<IntWritable, NullWritable, NullWritable, NullWritable>,
             Partitioner<IntWritable,NullWritable> {
  static final Log LOG = LogFactory.getLog(SleepJob.class);

  private long mapDuration = 1000;
  private long reduceDuration = 1000;
  private long recSleepTime = 100;
  private int mapMemMb = 0;
  private int mapBaseMb = 70; // default for sun jdk
  private int reduceMemMb = 0;
  private int reduceBaseMb = 74; // ditto
  private int memSpeedMbps = 188; // high estimate of random shuffle speed
  private long mem[];
  private float overhead = 1.5f; // default JVM overhead
  private Random rng;
  private int mapSleepCount = 1;
  private int reduceSleepCount = 1;
  private long startTime = System.currentTimeMillis();

  @Override
  public int getPartition(IntWritable k, NullWritable v, int numPartitions) {
    return k.get() % numPartitions;
  }

  public static class EmptySplit implements InputSplit {
    @Override
    public void write(DataOutput out) throws IOException { }
    @Override
    public void readFields(DataInput in) throws IOException { }
    @Override
    public long getLength() { return 0L; }
    @Override
    public String[] getLocations() { return new String[0]; }
  }

  public static class SleepInputFormat extends Configured
      implements InputFormat<IntWritable,IntWritable> {
    @Override
    public InputSplit[] getSplits(JobConf conf, int numSplits) {
      InputSplit[] ret = new InputSplit[numSplits];
      for (int i = 0; i < numSplits; ++i) {
        ret[i] = new EmptySplit();
      }
      return ret;
    }
    @Override
    public RecordReader<IntWritable,IntWritable> getRecordReader(
        InputSplit ignored, JobConf conf, Reporter reporter)
        throws IOException {
      final int count = conf.getInt("sleep.job.map.sleep.count", 1);
      if (count < 0) throw new IOException("Invalid map count: " + count);
      final int redcount = conf.getInt("sleep.job.reduce.sleep.count", 1);
      if (redcount < 0)
        throw new IOException("Invalid reduce count: " + redcount);
      final int emitPerMapTask = (redcount * conf.getNumReduceTasks());
    return new RecordReader<IntWritable,IntWritable>() {
        private int records = 0;
        private int emitCount = 0;

        @Override
        public boolean next(IntWritable key, IntWritable value)
            throws IOException {
          key.set(emitCount);
          int emit = emitPerMapTask / count;
          if ((emitPerMapTask) % count > records) {
            ++emit;
          }
          emitCount += emit;
          value.set(emit);
          return records++ < count;
        }
        @Override
        public IntWritable createKey() { return new IntWritable(); }
        @Override
        public IntWritable createValue() { return new IntWritable(); }
        @Override
        public long getPos() throws IOException { return records; }
        @Override
        public void close() throws IOException { }
        @Override
        public float getProgress() throws IOException {
          return records / ((float)count);
        }
      };
    }
  }

  @Override
  public void map(IntWritable key, IntWritable value,
      OutputCollector<IntWritable, NullWritable> output, Reporter reporter)
      throws IOException {

    // It is expected that every map processes mapSleepCount number of records.
    // The check lessens the impact of variations in memory shuffle speed.
    long elapsed = System.currentTimeMillis() - startTime;
    if (elapsed < mapDuration) try {
      reporter.setStatus((mapDuration - elapsed) +" ms left");
      useMapMemory();
      Thread.sleep(recSleepTime);
    }
    catch (InterruptedException ex) {
      throw new IOException("Interrupted while sleeping", ex);
    }
    // output reduceSleepCount * numReduce number of random values, so that
    // each reducer will get reduceSleepCount number of keys.
    int k = key.get();
    for (int i = 0; i < value.get(); ++i) {
      output.collect(new IntWritable(k + i), NullWritable.get());
    }
  }

  @Override
  public void reduce(IntWritable key, Iterator<NullWritable> values,
      OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
      throws IOException {
    long elapsed = System.currentTimeMillis() - startTime;
    if (elapsed < reduceDuration) try {
      reporter.setStatus((reduceDuration - elapsed) + " ms left");
      useReduceMemory();
      Thread.sleep(recSleepTime);
    }
    catch (InterruptedException ex) {
      throw (IOException)new IOException(
          "Interrupted while sleeping").initCause(ex);
    }
  }

  private void useMapMemory() {
    if (mapMemMb > 0) {
      allocMemIfNeeded(mapMemMb, mapBaseMb);
      useMemory();
    }
  }

  private void useReduceMemory() {
    if (reduceMemMb > 0) {
      allocMemIfNeeded(reduceMemMb, reduceBaseMb);
      useMemory();
    }
  }

  private void useMemory() {
    // Fisher-Yates shuffle to keep the memory pressure
    long startTime = System.currentTimeMillis();
    for (int i = mem.length - 1; i > 0; --i) {
      long tmp = mem[i];
      int j = rng.nextInt(i + 1); // n in nextInt(n) is exclusive
      mem[i] = mem[j];
      mem[j] = tmp;
    }
    LOG.info("random memory shuffle speed: "+ mem.length/1024./1024*8000/
        (System.currentTimeMillis()-startTime) + " MB/s");
  }

  private long[] allocMemIfNeeded(int mb, int base) {
    if (mem == null) {
      mem = new long[(int)(Math.max(1, mb - base) / 8. * 1024 * 1024)];
      rng = new Random();
    }
    return mem;
  }

  @Override
  public void configure(JobConf job) {
    this.mapSleepCount =
      job.getInt("sleep.job.map.sleep.count", mapSleepCount);
    this.reduceSleepCount =
      job.getInt("sleep.job.reduce.sleep.count", reduceSleepCount);
    this.mapDuration =
      job.getLong("sleep.job.map.sleep.time" , mapDuration);
    this.reduceDuration =
      job.getLong("sleep.job.reduce.sleep.time" , reduceDuration);
    this.recSleepTime = job.getLong("sleep.job.record.sleep.time", recSleepTime);
    this.mapMemMb = job.getInt("sleep.job.map.mem.mb", mapMemMb);
    this.reduceMemMb = job.getInt("sleep.job.reduce.mem.mb", reduceMemMb);
    this.mapBaseMb = job.getInt("sleep.job.map.mem.base.mb", mapBaseMb);
    this.reduceBaseMb = job.getInt("sleep.job.reduce.mem.base.mb", reduceBaseMb);
    if (job.getInt("mapred.reduce.tasks", 0) > 0) {
      mapBaseMb += 100; // maps somehow use more memory when numReducers > 0
    }
  }

  @Override
  public void close() throws IOException {
  }

  public static void main(String[] args) throws Exception{
    int res = ToolRunner.run(new Configuration(), new SleepJob(), args);
    System.exit(res);
  }

  public int run(int numMapper, int numReducer, long mapSleepTime,
      int mapSleepCount, long reduceSleepTime,
      int reduceSleepCount) throws IOException {
    JobConf job = setupJobConf(numMapper, numReducer, mapSleepTime, 
                  mapSleepCount, reduceSleepTime, reduceSleepCount);
    JobClient.runJob(job);
    return 0;
  }

  public JobConf setupJobConf(int numMapper, int numReducer, 
                                long mapSleepTime, int mapSleepCount, 
                                long reduceSleepTime, int reduceSleepCount) {
    JobConf job = new JobConf(getConf(), SleepJob.class);
    job.setNumMapTasks(numMapper);
    job.setNumReduceTasks(numReducer);
    job.setMapperClass(SleepJob.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setReducerClass(SleepJob.class);
    job.setOutputFormat(NullOutputFormat.class);
    job.setInputFormat(SleepInputFormat.class);
    job.setPartitionerClass(SleepJob.class);
    job.setSpeculativeExecution(false);
    job.setJobName("Sleep job");
    FileInputFormat.addInputPath(job, new Path("ignored"));
    job.setLong("sleep.job.map.sleep.time", mapSleepTime);
    job.setLong("sleep.job.reduce.sleep.time", reduceSleepTime);
    job.setLong("sleep.job.record.sleep.time", recSleepTime);
    job.setInt("sleep.job.map.mem.mb", mapMemMb);
    job.setInt("sleep.job.reduce.mem.mb", reduceMemMb);
    job.setInt("sleep.job.map.sleep.count", mapSleepCount);
    job.setInt("sleep.job.reduce.sleep.count", reduceSleepCount);
    if (mapMemMb > 0) {
      job.set("mapred.map.child.java.opts",
          "-Xmx" + (int)Math.max(512, mapMemMb * overhead) + "m");
    }
    if (reduceMemMb > 0) {
      job.set("mapred.reduce.child.java.opts",
          "-Xmx" + (int)Math.max(512, reduceMemMb * overhead) + "m");
    }
    if (mapMemMb > 0 || reduceMemMb > 0) {
      job.setInt("mapred.child.ulimit",
          (int)(Math.max(512, Math.max(mapMemMb, reduceMemMb)) * overhead * 3000));
    }
    return job;
  }

  @Override
  public int run(String[] args) throws Exception {

    if(args.length < 1) {
      System.err.println("sleep [-m numMappers] [-r numReducers]" +
          " [-mt mapTime (msec)] [-mm mapMemory (mb)]\n" +
          "      [-rt reduceTime (msec)] [-rm reduceMemory (mb)]\n" +
          "      [-recordt recordTime (msec)] [-mo memoryOverhead (~1.5)]\n");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    int numMapper = 1, numReducer = 1;
    long mapSleepTime = 100, reduceSleepTime = 100;
    int mapSleepCount = 1, reduceSleepCount = 1;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-m")) {
        numMapper = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-r")) {
        numReducer = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-mt")) {
        mapSleepTime = Long.parseLong(args[++i]);
      } else if (args[i].equals("-rt")) {
        reduceSleepTime = Long.parseLong(args[++i]);
      } else if (args[i].equals("-mm")) {
        mapMemMb = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-rm")) {
        reduceMemMb = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-mo")) {
        overhead = Float.parseFloat(args[++i]);
      } else if (args[i].equals("-recordt")) {
        recSleepTime = Long.parseLong(args[++i]);
      }
    }

    // Expected elapsed time in Task: recSleepTime + memory shuffle time per record
    mapSleepCount = (int)Math.ceil(mapSleepTime /
        (recSleepTime + Math.max(0, mapMemMb - mapBaseMb) * 1000. / memSpeedMbps));
    reduceSleepCount = (int)Math.ceil(reduceSleepTime /
        (recSleepTime + Math.max(0, reduceMemMb - reduceBaseMb) * 1000. / memSpeedMbps));
    LOG.info("mapCount:"+ mapSleepCount +"\treduceCount:"+ reduceSleepCount);

    return run(numMapper, numReducer, mapSleepTime, mapSleepCount,
        reduceSleepTime, reduceSleepCount);
  }
}
