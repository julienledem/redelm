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

import static junit.framework.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import redelm.hadoop.metadata.BlockMetaData;
import redelm.hadoop.metadata.ColumnChunkMetaData;
import redelm.hadoop.metadata.CompressionCodecName;
import redelm.hadoop.metadata.FileMetaData;
import redelm.io.RecordMaterializer;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType.PrimitiveTypeName;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

public class TestInputFormat {

  @Test
  public void testBlocksToSplits() throws IOException, InterruptedException {
    List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();
    for (int i = 0; i < 10; i++) {
      blocks.add(newBlock(i * 10));
    }
    ReadSupport<Void> readSupport = new ReadSupport<Void>() {
      @Override
      public RecordMaterializer<Void> newRecordConsumer() {
        return null;
      }
      @Override
      public void initForRead(Map<String, String> keyValueMetaDatq,
          String requestedSchema) {
      }
    };
    BlockLocation[] hdfsBlocks = new BlockLocation[] {
        new BlockLocation(new String[0], new String[] { "foo0.datanode", "bar0.datanode"}, 0, 50),
        new BlockLocation(new String[0], new String[] { "foo1.datanode", "bar1.datanode"}, 50, 50)
    };
    FileStatus fileStatus = new FileStatus(100, false, 2, 50, 0, new Path("hdfs://foo.namenode:1234/bar"));
    FileMetaData fileMetaData = new FileMetaData(new MessageType("foo"));
    List<InputSplit> splits = RedelmInputFormat.generateSplits(blocks, hdfsBlocks, fileStatus, fileMetaData, readSupport);
    assertEquals(splits.toString().replaceAll("([{])", "$0\n").replaceAll("([}])", "\n$0"), 2, splits.size());
    for (int i = 0; i < splits.size(); i++) {
      RedelmInputSplit<?> redelmInputSplit = (RedelmInputSplit<?>)splits.get(i);
      assertEquals(5, redelmInputSplit.getBlocks().size());
      assertEquals(2, redelmInputSplit.getLocations().length);
      assertEquals("[foo" + i + ".datanode, bar" + i + ".datanode]", Arrays.toString(redelmInputSplit.getLocations()));
      assertEquals(50, redelmInputSplit.getLength());
    }
  }

  private BlockMetaData newBlock(long start) {
    BlockMetaData blockMetaData = new BlockMetaData();
    ColumnChunkMetaData column = new ColumnChunkMetaData(new String[] {"foo"}, PrimitiveTypeName.BINARY, CompressionCodecName.GZIP);
    column.setFirstDataPage(start);
    blockMetaData.addColumn(column);
    return blockMetaData;
  }
}
