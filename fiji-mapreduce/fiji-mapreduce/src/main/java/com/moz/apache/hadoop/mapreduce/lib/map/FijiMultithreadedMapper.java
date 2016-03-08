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

package org.apache.hadoop.mapreduce.lib.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.moz.fiji.mapreduce.platform.FijiMRPlatformBridge;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiRowData;

/**
 * Multithreaded implementation for @link org.apache.hadoop.mapreduce.Mapper.
 * <p>
 * It can be used instead of the default implementation,
 * @link org.apache.hadoop.mapred.MapRunner, when the Map operation is not CPU
 * bound in order to improve throughput.
 * <p>
 * Mapper implementations using this MapRunnable must be thread-safe.
 * <p>
 * The MapReduce job has to be configured with the mapper to use via
 * {@link #setMapperClass(Configuration, Class)} and
 * the number of thread the thread-pool can use with the
 * {@link #getNumberOfThreads(Configuration) method. The default
 * value is 10 threads.
 * <p>
 */
public class FijiMultithreadedMapper<K, V>
  extends Mapper<EntityId, FijiRowData, K, V> {

  private static final Log LOG = LogFactory.getLog(MultithreadedMapper.class);
  private Class<? extends Mapper<EntityId,FijiRowData,K,V>> mapClass;
  private Context outer;
  private List<MapRunner> runners;

  /**
   * The number of threads in the thread pool that will run the map function.
   * @param job the job
   * @return the number of threads
   */
  public static int getNumberOfThreads(JobContext job) {
    return job.getConfiguration().getInt("mapred.map.multithreadedrunner.threads", 10);
  }

  /**
   * Set the number of threads in the pool for running maps.
   * @param job the job to modify
   * @param threads the new number of threads
   */
  public static void setNumberOfThreads(Job job, int threads) {
    job.getConfiguration().setInt("mapred.map.multithreadedrunner.threads", threads);
  }

  /**
   * Get the application's mapper class.
   * @param <K> the map's output key type
   * @param <V> the map's output value type
   * @param job the job
   * @return the mapper class to run
   */
  @SuppressWarnings("unchecked")
  public static <K,V> Class<Mapper<EntityId,FijiRowData,K,V>> getMapperClass(JobContext job) {
    return (Class<Mapper<EntityId,FijiRowData,K,V>>)
         job.getConfiguration().getClass("mapred.map.multithreadedrunner.class", Mapper.class);
  }

  /**
   * Set the application's mapper class.
   * @param <K> the map output key type
   * @param <V> the map output value type
   * @param job the job to modify
   * @param cls the class to use as the mapper
   */
  public static <K,V> void setMapperClass(Job job,
      Class<? extends Mapper<EntityId,FijiRowData,K,V>> cls) {

    if (MultithreadedMapper.class.isAssignableFrom(cls)) {
      throw new IllegalArgumentException("Can't have recursive " +
          "MultithreadedMapper instances.");
    }
    job.getConfiguration().setClass("mapred.map.multithreadedrunner.class", cls, Mapper.class);
  }

  /**
   * Run the application's maps using a thread pool.
   */
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    outer = context;
    int numberOfThreads = getNumberOfThreads(context);
    mapClass = getMapperClass(context);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Configuring multithread runner to use " + numberOfThreads + " threads");
    }

    runners =  new ArrayList<MapRunner>(numberOfThreads);
    for(int i=0; i < numberOfThreads; ++i) {
      MapRunner thread = new MapRunner(context);
      thread.start();
      runners.add(i, thread);
    }
    for(int i=0; i < numberOfThreads; ++i) {
      MapRunner thread = runners.get(i);
      thread.join();
      Throwable th = thread.throwable;
      if (th != null) {
        if (th instanceof IOException) {
          throw (IOException) th;
        } else if (th instanceof InterruptedException) {
          throw (InterruptedException) th;
        } else {
          throw new RuntimeException(th);
        }
      }
    }
  }

  private class SubMapRecordReader extends RecordReader<EntityId,FijiRowData> {
    private EntityId key;
    private FijiRowData value;
    private Configuration conf;

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {

      conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      synchronized (outer) {
        if (!outer.nextKeyValue()) {
          return false;
        }

        key = outer.getCurrentKey();
        value = outer.getCurrentValue();
        return true;
      }
    }

    public EntityId getCurrentKey() {
      return key;
    }

    @Override
    public FijiRowData getCurrentValue() {
      return value;
    }
  }

  private class SubMapRecordWriter extends RecordWriter<K,V> {

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      synchronized (outer) {
        outer.write(key, value);
      }
    }
  }

  private class SubMapStatusReporter extends StatusReporter {

    @Override
    public Counter getCounter(Enum<?> name) {
      return outer.getCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
      return outer.getCounter(group, name);
    }

    @Override
    public void progress() {
      outer.progress();
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void setStatus(String status) {
      outer.setStatus(status);
    }
  }

  private class MapRunner extends Thread {
    private Mapper<EntityId,FijiRowData,K,V> mapper;
    private Context subcontext;
    private Throwable throwable;

    MapRunner(Context context) throws IOException, InterruptedException {
      mapper = ReflectionUtils.newInstance(mapClass, context.getConfiguration());
      subcontext = FijiMRPlatformBridge.get().getMapperContext(
          outer.getConfiguration(),
          outer.getTaskAttemptID(),
          new SubMapRecordReader(),
          new SubMapRecordWriter(),
          context.getOutputCommitter(),
          new SubMapStatusReporter(),
          outer.getInputSplit());
    }

    public Throwable getThrowable() {
      return throwable;
    }

    @Override
    public void run() {
      try {
        mapper.run(subcontext);
      } catch (Throwable ie) {
        throwable = ie;
      }
    }
  }
}
