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

import static redelm.Log.DEBUG;
import static redelm.Log.INFO;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import redelm.Log;
import redelm.bytes.BytesInput;
import redelm.bytes.BytesUtils;
import redelm.column.ColumnDescriptor;
import redelm.format.converter.ParquetMetadataConverter;
import redelm.hadoop.metadata.BlockMetaData;
import redelm.hadoop.metadata.ColumnChunkMetaData;
import redelm.hadoop.metadata.CompressionCodecName;
import redelm.hadoop.metadata.FileMetaData;
import redelm.hadoop.metadata.RedelmMetaData;
import redelm.schema.MessageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.format.DataPageHeader;
import parquet.format.Encoding;
import parquet.format.PageHeader;
import parquet.format.PageType;

/**
 * Writes a RedElm file
 * @author Julien Le Dem
 *
 */
public class RedelmFileWriter {
  private static final Log LOG = Log.getLog(RedelmFileWriter.class);

  public static final String RED_ELM_SUMMARY = "_RedElmSummary";
  public static final byte[] MAGIC = "PAR1".getBytes(Charset.forName("ASCII"));
  public static final int CURRENT_VERSION = 1;

  private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private final MessageType schema;
  private final FSDataOutputStream out;
  private BlockMetaData currentBlock;
  private ColumnChunkMetaData currentColumn;
  private long currentRecordCount;
  private List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();
  private long uncompressedLength;
  private long compressedLength;
  private final ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();

  /**
   * Captures the order in which methods should be called
   *
   * @author Julien Le Dem
   *
   */
  private enum STATE {
    NOT_STARTED {
      STATE start() {
        return STARTED;
      }
    },
    STARTED {
      STATE startBlock() {
        return BLOCK;
      }
      STATE end() {
        return ENDED;
      }
    },
    BLOCK  {
      STATE startColumn() {
        return COLUMN;
      }
      STATE endBlock() {
        return STARTED;
      }
    },
    COLUMN {
      STATE endColumn() {
        return BLOCK;
      };
      STATE write() {
        return this;
      }
    },
    ENDED;

    STATE start() {throw new IllegalStateException(this.name());}
    STATE startBlock() {throw new IllegalStateException(this.name());}
    STATE startColumn() {throw new IllegalStateException(this.name());}
    STATE write() {throw new IllegalStateException(this.name());}
    STATE endColumn() {throw new IllegalStateException(this.name());}
    STATE endBlock() {throw new IllegalStateException(this.name());}
    STATE end() {throw new IllegalStateException(this.name());}
  }

  private STATE state = STATE.NOT_STARTED;

  /**
   *
   * @param schema the schema of the data
   * @param out the file to write to
   * @param codec the codec to use to compress blocks
   * @throws IOException if the file can not be created
   */
  public RedelmFileWriter(Configuration configuration, MessageType schema, Path file) throws IOException {
    super();
    this.schema = schema;
    FileSystem fs = file.getFileSystem(configuration);
    this.out = fs.create(file, false);
  }

  /**
   * start the file
   * @throws IOException
   */
  public void start() throws IOException {
    state = state.start();
    if (DEBUG) LOG.debug(out.getPos() + ": start");
    out.write(MAGIC);
  }

  /**
   * start a block
   * @param recordCount the record count in this block
   * @throws IOException
   */
  public void startBlock(long recordCount) throws IOException {
    state = state.startBlock();
    if (DEBUG) LOG.debug(out.getPos() + ": start block");
//    out.write(MAGIC); // TODO: add a magic delimiter
    currentBlock = new BlockMetaData();
    currentRecordCount = recordCount;
  }

  /**
   * start a column inside a block
   * @param descriptor the column descriptor
   * @param valueCount the value count in this column
   * @param compressionCodecName
   * @throws IOException
   */
  public void startColumn(ColumnDescriptor descriptor, long valueCount, CompressionCodecName compressionCodecName) throws IOException {
    state = state.startColumn();
    if (DEBUG) LOG.debug(out.getPos() + ": start column: " + descriptor + " count=" + valueCount);
    currentColumn = new ColumnChunkMetaData(descriptor.getPath(), descriptor.getType(), compressionCodecName);
    currentColumn.setValueCount(valueCount);
    currentColumn.setFirstDataPage(out.getPos());
    compressedLength = 0;
    uncompressedLength = 0;
  }

  /**
   * writes a single page
   * @param valueCount count of values
   * @param uncompressedPageSize the size of the data once uncompressed
   * @param bytes the compressed data for the page without header
   */
  public void writeDataPage(
      int valueCount, int uncompressedPageSize,
      BytesInput bytes) throws IOException {
    state = state.write();
    if (DEBUG) LOG.debug(out.getPos() + ": write data page: " + valueCount + " values");
    int compressedPageSize = (int)bytes.size();
    PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, uncompressedPageSize, compressedPageSize);
    // pageHeader.crc = ...;
    pageHeader.data_page = new DataPageHeader(valueCount, Encoding.PLAIN); // TODO: encoding
    metadataConverter.writePageHeader(pageHeader, out);
    this.uncompressedLength += uncompressedPageSize;
    this.compressedLength += compressedPageSize;
    if (DEBUG) LOG.debug(out.getPos() + ": write data page content " + compressedPageSize);
    bytes.writeAllTo(out);
  }

  /**
   * writes a number of pages at once
   * @param bytes bytes to be written including page headers
   * @param uncompressedTotalPageSize total uncompressed size (without page headers)
   * @param compressedTotalPageSize total compressed size (without page headers)
   * @throws IOException
   */
   void writeDataPages(BytesInput bytes, long uncompressedTotalPageSize, long compressedTotalPageSize) throws IOException {
    state = state.write();
    if (DEBUG) LOG.debug(out.getPos() + ": write data pages");
//    int compressedPageSize = (int)bytes.size();
//    PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, uncompressedPageSize, compressedPageSize);
//    // pageHeader.crc = ...;
//    pageHeader.data_page = new DataPageHeader(valueCount, Encoding.PLAIN); // TODO: encoding
//    metadataConverter.writePageHeader(pageHeader, out);
    this.uncompressedLength += uncompressedTotalPageSize;
    this.compressedLength += compressedTotalPageSize;
    if (DEBUG) LOG.debug(out.getPos() + ": write data pages content");
    bytes.writeAllTo(out);
  }

  /**
   * end a column (once all rep, def and data have been written)
   * @throws IOException
   */
  public void endColumn() throws IOException {
    state = state.endColumn();
    if (DEBUG) LOG.debug(out.getPos() + ": end column");
    currentColumn.setTotalUncompressedSize(uncompressedLength);
    currentColumn.setTotalSize(compressedLength);
    currentBlock.addColumn(currentColumn);
    if (INFO) LOG.info("ended Column chumk: " + currentColumn);
    currentColumn = null;
    this.uncompressedLength = 0;
    this.compressedLength = 0;
  }

  /**
   * ends a block once all column chunks have been written
   * @throws IOException
   */
  public void endBlock() throws IOException {
    state = state.endBlock();
    if (DEBUG) LOG.debug(out.getPos() + ": end block");
    currentBlock.setRowCount(currentRecordCount);
    blocks.add(currentBlock);
    currentBlock = null;
  }

  /**
   * ends a file once all blocks have been written.
   * closes the file.
   * @param extraMetaData the extra meta data to write in the footer
   * @throws IOException
   */
  public void end(Map<String, String> extraMetaData) throws IOException {
    state = state.end();
    if (DEBUG) LOG.debug(out.getPos() + ": end");
    long footerIndex = out.getPos();
    RedelmMetaData footer = new RedelmMetaData(new FileMetaData(schema), blocks, extraMetaData);
    serializeFooter(footer, out);
    if (DEBUG) LOG.debug(out.getPos() + ": footer length = " + (out.getPos() - footerIndex));
    BytesUtils.writeIntLittleEndian(out, (int)(out.getPos() - footerIndex));
    out.write(MAGIC);
    out.close();
  }

  private void serializeFooter(RedelmMetaData footer, OutputStream os) throws IOException {
    parquet.format.FileMetaData parquetMetadata = new ParquetMetadataConverter().toParquetMetadata(CURRENT_VERSION, footer);
    metadataConverter.writeFileMetaData(parquetMetadata, os);
  }

  public static void writeSummaryFile(Configuration configuration, Path outputPath, List<Footer> footers) throws IOException {
    Path summaryPath = new Path(outputPath, RED_ELM_SUMMARY);
    FileSystem fs = outputPath.getFileSystem(configuration);
    FSDataOutputStream summary = fs.create(summaryPath);
    summary.writeInt(footers.size());
    for (Footer footer : footers) {
      summary.writeUTF(footer.getFile().toString());
      parquet.format.FileMetaData parquetMetadata = parquetMetadataConverter.toParquetMetadata(CURRENT_VERSION, footer.getRedelmMetaData());
      parquetMetadataConverter.writeFileMetaData(parquetMetadata, summary);
    }
    summary.close();
  }

}
