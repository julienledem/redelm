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
package parquet.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.ColumnDescriptor;
import parquet.column.mem.PageWriteStore;
import parquet.column.mem.PageWriter;
import parquet.format.DataPageHeader;
import parquet.format.Encoding;
import parquet.format.PageHeader;
import parquet.format.PageType;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.CodecFactory.BytesCompressor;
import parquet.schema.MessageType;

public class ColumnChunkPageWriteStore implements PageWriteStore {

  private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private static final class ColumnChunkPageWriter implements PageWriter {

    private final ColumnDescriptor path;
    private final BytesCompressor compressor;

    private final CapacityByteArrayOutputStream buf;

    private long uncompressedLength;
    private long compressedLength;
    private long totalValueCount;

    private ColumnChunkPageWriter(ColumnDescriptor path, BytesCompressor compressor, int initialSize) {
      this.path = path;
      this.compressor = compressor;
      this.buf = new CapacityByteArrayOutputStream(initialSize);
    }

    @Override
    public void writePage(BytesInput bytes, int valueCount)
        throws IOException {
      long uncompressedSize = bytes.size();
      BytesInput compressedBytes = compressor.compress(bytes);
      long compressedSize = compressedBytes.size();
      PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, (int)uncompressedSize, (int)compressedSize);
      // pageHeader.crc = ...;
      pageHeader.data_page = new DataPageHeader(valueCount, Encoding.PLAIN); // TODO: encoding
      parquetMetadataConverter.writePageHeader(pageHeader, buf);
      this.uncompressedLength += uncompressedSize;
      this.compressedLength += compressedSize;
      this.totalValueCount += valueCount;
      compressedBytes.writeAllTo(buf);
    }

    @Override
    public long getMemSize() {
      return buf.size();
    }

    public void writeToFileWriter(ParquetFileWriter writer) throws IOException {
      writer.startColumn(path, totalValueCount, compressor.getCodecName());
      writer.writeDataPages(BytesInput.from(buf), uncompressedLength, compressedLength);
      writer.endColumn();
    }

    @Override
    public long allocatedSize() {
      return buf.getCapacity();
    }
  }

  private final Map<ColumnDescriptor, ColumnChunkPageWriter> writers = new HashMap<ColumnDescriptor, ColumnChunkPageWriter>();
  private final MessageType schema;
  private final BytesCompressor compressor;

  public ColumnChunkPageWriteStore(BytesCompressor compressor, MessageType schema) {
    this.compressor = compressor;
    this.schema = schema;
  }

  @Override
  public PageWriter getPageWriter(ColumnDescriptor path) {
    if (!writers.containsKey(path)) {
      writers.put(path,  new ColumnChunkPageWriter(path, compressor, 1024*1024/2)); // TODO: better deal with this initial size
    }
    return writers.get(path);
  }

  public void flushToFileWriter(ParquetFileWriter writer) throws IOException {
    List<ColumnDescriptor> columns = schema.getColumns();
    for (ColumnDescriptor columnDescriptor : columns) {
      ColumnChunkPageWriter pageWriter = writers.get(columnDescriptor);
      pageWriter.writeToFileWriter(writer);
    }
  }

}
