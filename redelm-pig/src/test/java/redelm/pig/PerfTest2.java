/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package redelm.pig;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;

public class PerfTest2 {
  private static final int COLUMN_COUNT = 50;
  private static final long ROW_COUNT = 100000;
  private static StringBuilder results = new StringBuilder();
  private static Configuration conf = new Configuration();
  private static int jobid = 0;

  public static void main(String[] args) throws Exception {
    String out = "target/PerfTest2";
    File outDir = new File(out);
    if (outDir.exists()) {
      clean(outDir);
    }
    {
      StringBuilder schemaString = new StringBuilder("a0: chararray");
      for (int i = 1; i < COLUMN_COUNT; i++) {
        schemaString.append(", a" + i + ": chararray");
      }

      String location = out;
      String schema = schemaString.toString();

      StoreFuncInterface storer = new RedelmStorer();
      Job job = new Job(conf);
      storer.setStoreFuncUDFContextSignature("sig");
      String absPath = storer.relToAbsPathForStoreLocation(location, new Path(new File(".").getAbsoluteFile().toURI()));
      storer.setStoreLocation(absPath, job);
      storer.checkSchema(new ResourceSchema(Utils.getSchemaFromString(schema)));
      OutputFormat outputFormat = storer.getOutputFormat();
      // it's job.getConfiguration() and not just conf !
      JobContext jobContext = new JobContext(job.getConfiguration(), new JobID("jt", jobid ++));
      outputFormat.checkOutputSpecs(jobContext);
      if (schema != null) {
        ResourceSchema resourceSchema = new ResourceSchema(Utils.getSchemaFromString(schema));
        storer.checkSchema(resourceSchema);
        if (storer instanceof StoreMetadata) {
          ((StoreMetadata)storer).storeSchema(resourceSchema, absPath, job);
        }
      }
      TaskAttemptContext taskAttemptContext = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID("jt", jobid, true, 1, 0));
      RecordWriter recordWriter = outputFormat.getRecordWriter(taskAttemptContext);
      storer.prepareToWrite(recordWriter);

      for (int i = 0; i < ROW_COUNT; i++) {
        Tuple tuple = TupleFactory.getInstance().newTuple(COLUMN_COUNT);
        for (int j = 0; j < COLUMN_COUNT; j++) {
          tuple.set(j, "a" + i + "_" + j);
        }
        storer.putNext(tuple);
      }

      recordWriter.close(taskAttemptContext);
      OutputCommitter outputCommitter = outputFormat.getOutputCommitter(taskAttemptContext);
      outputCommitter.commitTask(taskAttemptContext);
      outputCommitter.commitJob(jobContext);

    }
    for (int i = 0; i < 2; i++) {

    load(out, 1);
    load(out, 2);
    load(out, 3);
    load(out, 4);
    load(out, 5);
    load(out, 10);
    load(out, 20);
    load(out, 50);
    results.append("\n");
    }
    System.out.println(results);
  }

  private static void clean(File outDir) {
    if (outDir.isDirectory()) {
      File[] listFiles = outDir.listFiles();
      for (File file : listFiles) {
        clean(file);
      }
    }
    outDir.delete();
  }

  private static void load(String out, int colsToLoad) throws Exception {
    StringBuilder schemaString = new StringBuilder("a0: chararray");
    for (int i = 1; i < colsToLoad; i++) {
      schemaString.append(", a" + i + ": chararray");
    }

    long t0 = System.currentTimeMillis();
    Job job = new Job(conf);
    int loadjobId = jobid ++;
    LoadFunc loadFunc = new RedelmLoader(schemaString.toString());
    loadFunc.setUDFContextSignature("sigLoader"+loadjobId);
    String absPath = loadFunc.relativeToAbsolutePath(out, new Path(new File(".").getAbsoluteFile().toURI()));
    loadFunc.setLocation(absPath, job);
    InputFormat inputFormat = loadFunc.getInputFormat();
    JobContext jobContext = new JobContext(job.getConfiguration(), new JobID("jt", loadjobId));
    List splits = inputFormat.getSplits(jobContext);
    int i = 0;
    int taskid = 0;
    for (Object split : splits) {
      TaskAttemptContext taskAttemptContext = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID("jt", loadjobId, true, taskid++, 0));
      RecordReader recordReader = inputFormat.createRecordReader((InputSplit)split, taskAttemptContext);
      loadFunc.prepareToRead(recordReader, null);
      recordReader.initialize((InputSplit)split, taskAttemptContext);
      Tuple t;
      while ((t = loadFunc.getNext()) != null) {
//      System.out.println(t);
        ++i;
      }
    }
    assertEquals(ROW_COUNT, i);
    long t1 = System.currentTimeMillis();
    results.append((t1-t0)+" ms to read "+colsToLoad+" columns\n");
  }

}
