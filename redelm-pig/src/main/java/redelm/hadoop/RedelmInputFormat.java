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
package redelm.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import redelm.Log;
import redelm.hadoop.RedelmMetaData.FileMetaData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * The input format to read a RedElm file
 *
 * It requires an implementation of {@link ReadSupport} to materialize the records
 *
 * The requestedSchema will control how the original records get projected by the loader.
 * It must be a subset of the original schema. Only the columns needed to reconstruct the records with the requestedSchema will be scanned.
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized records
 */
public class RedelmInputFormat<T> extends FileInputFormat<Void, T> {

  private static final Log LOG = Log.getLog(RedelmInputFormat.class);

  private String requestedSchema;
  private Class<?> readSupportClass;

  /**
   * constructor used when this InputFormat in wrapped in another one (In Pig for example)
   * TODO: stand-alone constructor
   * @param readSupportClass the class to materialize records
   * @param requestedSchema the schema use to project the records (must be a subset of the original schema)
   */
  public <S extends ReadSupport<T>> RedelmInputFormat(Class<S> readSupportClass, String requestedSchema) {
    this.readSupportClass = readSupportClass;
    this.requestedSchema = requestedSchema;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordReader<Void, T> createRecordReader(
      InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    @SuppressWarnings("unchecked") // I know
    RedelmInputSplit<T> redelmInputSplit = (RedelmInputSplit<T>)inputSplit;
    return new RedelmRecordReader<T>(getRequestedSchema(redelmInputSplit.getFileMetaData()));
  }

  private String getRequestedSchema(FileMetaData fileMetaData) {
    return requestedSchema == null ?
        fileMetaData.getSchema() :
        requestedSchema;
  }

  /**
   * groups together all the data blocks for the same HDFS block
   * @param blocks data blocks (row groups)
   * @param hdfsBlocks hdfs blocks
   * @param fileStatus the containing file
   * @param fileMetaData file level meta data
   * @param readSupport how to materialize the records
   * @return the splits (one per HDFS block)
   * @throws IOException If hosts can't be retrieved for the HDFS block
   */
  static <T> List<InputSplit> generateSplits(List<BlockMetaData> blocks,
      BlockLocation[] hdfsBlocks, FileStatus fileStatus,
      FileMetaData fileMetaData, ReadSupport<T> readSupport) throws IOException {
    int currentBlock = 0;
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (BlockLocation hdfsBlock : hdfsBlocks) {
      long start = hdfsBlock.getOffset();
      long end = hdfsBlock.getOffset() + hdfsBlock.getLength();
      List<BlockMetaData> blocksForCurrentSplit = new ArrayList<BlockMetaData>();
      while (
          currentBlock < blocks.size()
          && blocks.get(currentBlock).getStartIndex() >= start
          && blocks.get(currentBlock).getStartIndex() < end) {
        blocksForCurrentSplit.add(blocks.get(currentBlock));
        ++ currentBlock;
      }
      if (blocksForCurrentSplit.size() > 0) {
        splits.add(new RedelmInputSplit<T>(
            fileStatus.getPath(),
            hdfsBlock.getOffset(),
            hdfsBlock.getLength(),
            hdfsBlock.getHosts(),
            blocksForCurrentSplit,
            fileMetaData,
            readSupport));
      }
    }
    return splits;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> statuses = super.listStatus(jobContext);
    LOG.debug("reading " + statuses.size() + " files");
    Configuration configuration = jobContext.getConfiguration();
    FileSystem fs = FileSystem.get(configuration);
    // TODO use summary files
    List<Footer> footers = RedelmFileReader.readAllFootersInParallel(configuration, statuses);
    for (Footer footer : footers) {
      LOG.debug(footer.getFile());
      try {
        @SuppressWarnings("unchecked")
        ReadSupport<T> readSupport = (ReadSupport<T>) readSupportClass.newInstance();
        FileStatus fileStatus = fs.getFileStatus(footer.getFile());
        List<MetaDataBlock> metaDataBlocks = footer.getMetaDataBlocks();
        RedelmMetaData redelmMetaData = RedelmMetaData.fromMetaDataBlocks(metaDataBlocks);
        readSupport.initForRead(
            metaDataBlocks,
            getRequestedSchema(redelmMetaData.getFileMetaData())
            );
        List<BlockMetaData> blocks = redelmMetaData.getBlocks();
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        splits.addAll(
            generateSplits(blocks, fileBlockLocations, fileStatus, redelmMetaData.getFileMetaData(), readSupport)
              );
      } catch (InstantiationException e) {
        throw new RuntimeException("could not instantiate " + readSupportClass.getName(), e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Illegal access to class " + readSupportClass.getName(), e);
      }
    }
    return splits;
  }

}
