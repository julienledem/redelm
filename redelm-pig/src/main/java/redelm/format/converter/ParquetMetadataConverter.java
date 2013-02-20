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
package redelm.format.converter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import redelm.hadoop.metadata.BlockMetaData;
import redelm.hadoop.metadata.ColumnChunkMetaData;
import redelm.hadoop.metadata.CompressionCodecName;
import redelm.hadoop.metadata.RedelmMetaData;
import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType;
import redelm.schema.PrimitiveType.PrimitiveTypeName;
import redelm.schema.Type.Repetition;
import redelm.schema.TypeVisitor;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import parquet.format.ColumnChunk;
import parquet.format.CompressionCodec;
import parquet.format.Encoding;
import parquet.format.FieldRepetitionType;
import parquet.format.FileMetaData;
import parquet.format.KeyValue;
import parquet.format.PageHeader;
import parquet.format.RowGroup;
import parquet.format.SchemaElement;
import parquet.format.Type;

public class ParquetMetadataConverter {

  public FileMetaData toParquetMetadata(int currentVersion, RedelmMetaData redelmMetadata) {
    List<BlockMetaData> blocks = redelmMetadata.getBlocks();
    List<RowGroup> rowGroups = new ArrayList<RowGroup>();
    int numRows = 0;
    for (BlockMetaData block : blocks) {
      numRows += block.getRowCount();
      addRowGroup(redelmMetadata, rowGroups, block);
    }
    FileMetaData fileMetaData = new FileMetaData(
        currentVersion,
        toParquetSchema(redelmMetadata.getFileMetaData().getSchema()),
        numRows,
        rowGroups
        );

    Set<Entry<String, String>> keyValues = redelmMetadata.getKeyValueMetaData().entrySet();
    for (Entry<String, String> keyValue : keyValues) {
      addKeyValue(fileMetaData, keyValue.getKey(), keyValue.getValue());
    }

    return fileMetaData;
  }

  List<SchemaElement> toParquetSchema(MessageType schema) {
    List<SchemaElement> result = new ArrayList<SchemaElement>();
    addToList(result, schema);
    return result;
  }

  private void addToList(final List<SchemaElement> result, redelm.schema.Type field) {
    field.accept(new TypeVisitor() {
      @Override
      public void visit(PrimitiveType primitiveType) {
        SchemaElement element = new SchemaElement(primitiveType.getName());
        element.setRepetition_type(toParquetRepetition(primitiveType.getRepetition()));
        element.setType(getType(primitiveType.getPrimitiveTypeName()));
        result.add(element);
      }

      private FieldRepetitionType toParquetRepetition(Repetition repetition) {
        switch (repetition) {
        case REQUIRED: return FieldRepetitionType.REQUIRED;
        case OPTIONAL: return FieldRepetitionType.OPTIONAL;
        case REPEATED: return FieldRepetitionType.REPEATED;
        }
        throw new RuntimeException("unknown repetition: " + repetition);
      }

      @Override
      public void visit(MessageType messageType) {
        SchemaElement element = new SchemaElement(messageType.getName());
        visitChildren(result, messageType.asGroupType(), element);
      }

      @Override
      public void visit(GroupType groupType) {
        SchemaElement element = new SchemaElement(groupType.getName());
        element.setRepetition_type(toParquetRepetition(groupType.getRepetition()));
        visitChildren(result, groupType, element);
      }

      private void visitChildren(final List<SchemaElement> result,
          GroupType groupType, SchemaElement element) {
        element.setNum_children(groupType.getFieldCount());
        result.add(element);
        for (redelm.schema.Type field : groupType.getFields()) {
          addToList(result, field);
        }
      }
    });
  }

  private void addRowGroup(RedelmMetaData redelmMetadata, List<RowGroup> rowGroups, BlockMetaData block) {
    //rowGroup.total_byte_size = ;
    List<ColumnChunkMetaData> columns = block.getColumns();
    List<ColumnChunk> parquetColumns = new ArrayList<ColumnChunk>();
    for (ColumnChunkMetaData columnMetaData : columns) {
      ColumnChunk columnChunk = new ColumnChunk(columnMetaData.getFirstDataPage()); // verify this is the right offset
      columnChunk.file_path = null; // same file
      columnChunk.meta_data = new parquet.format.ColumnMetaData(
          getType(columnMetaData.getType()),
          Arrays.asList(Encoding.PLAIN), // TODO: deal with encodings
          Arrays.asList(columnMetaData.getPath()),
          columnMetaData.getCodec().getParquetCompressionCodec(),
          columnMetaData.getValueCount(),
          columnMetaData.getTotalUncompressedSize(),
          columnMetaData.getTotalSize(),
          columnMetaData.getFirstDataPage()
          );
//      columnChunk.meta_data.index_page_offset = ;
//      columnChunk.meta_data.key_value_metadata = ; // nothing yet

      parquetColumns.add(columnChunk);
    }
    RowGroup rowGroup = new RowGroup(parquetColumns, block.getTotalByteSize(), block.getRowCount());
    rowGroups.add(rowGroup);
  }

  private CompressionCodec getCodec(String codecClassName) {
    if (codecClassName.equals("org.apache.hadoop.io.compress.GzipCodec")) {
      return CompressionCodec.GZIP;
    } else if (codecClassName.equals("com.hadoop.compression.lzo.LzopCodec")) {
      return CompressionCodec.LZO;
    } else if (codecClassName.equals("org.apache.hadoop.io.compress.SnappyCodec")) {
      return CompressionCodec.SNAPPY;
    } else if (codecClassName.equals("")) {
      return CompressionCodec.UNCOMPRESSED;
    } else {
      throw new RuntimeException("Unknown Codec "+ codecClassName);
    }
  }

  private PrimitiveTypeName getPrimitive(Type type) {
    switch (type) {
      case BYTE_ARRAY:
        return PrimitiveTypeName.BINARY;
      case INT64:
        return PrimitiveTypeName.INT64;
      case INT32:
        return PrimitiveTypeName.INT32;
      case BOOLEAN:
        return PrimitiveTypeName.BOOLEAN;
      case FLOAT:
        return PrimitiveTypeName.FLOAT;
      case DOUBLE:
        return PrimitiveTypeName.DOUBLE;
      default:
        throw new RuntimeException("Unknown type " + type);
    }
  }

  private Type getType(PrimitiveTypeName type) {
    switch (type) {
      case INT64:
        return Type.INT64;
      case INT32:
        return Type.INT32;
      case BOOLEAN:
        return Type.BOOLEAN;
      case BINARY:
        return Type.BYTE_ARRAY;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      default:
        throw new RuntimeException("Unknown type " + type);
    }
  }

  private void addKeyValue(FileMetaData fileMetaData, String key, String value) {
    KeyValue keyValue = new KeyValue(key);
    keyValue.value = value;
    fileMetaData.addToKey_value_metadata(keyValue);
  }

  public RedelmMetaData fromParquetMetadata(FileMetaData parquetMetadata) throws IOException {
//    List<MetaDataBlock> result = new ArrayList<MetaDataBlock>();
    MessageType messageType = fromParquetSchema(parquetMetadata.getSchema());
    redelm.hadoop.metadata.FileMetaData fileMetadata = new redelm.hadoop.metadata.FileMetaData(messageType);
    List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();
    List<RowGroup> row_groups = parquetMetadata.getRow_groups();
    for (RowGroup rowGroup : row_groups) {
      BlockMetaData blockMetaData = new BlockMetaData();
      blockMetaData.setRowCount(rowGroup.getNum_rows());
      blockMetaData.setTotalByteSize(rowGroup.getTotal_byte_size());
      List<ColumnChunk> columns = rowGroup.getColumns();
      for (ColumnChunk columnChunk : columns) {
        parquet.format.ColumnMetaData metaData = columnChunk.meta_data;
        String[] path = metaData.path_in_schema.toArray(new String[metaData.path_in_schema.size()]);
        ColumnChunkMetaData column = new ColumnChunkMetaData(path, messageType.getType(path).asPrimitiveType().getPrimitiveTypeName(), CompressionCodecName.fromParquet(metaData.codec));
        column.setFirstDataPage(metaData.data_page_offset);
        column.setValueCount(metaData.num_values);
        column.setTotalUncompressedSize(metaData.total_uncompressed_size);
        column.setTotalSize(metaData.total_compressed_size);
        // TODO
        // encodings
        // index_page_offset
        // key_value_metadata
        blockMetaData.addColumn(column);
      }
      blocks.add(blockMetaData);
    }
    Map<String, String> keyValueMetaData = new HashMap<String, String>();
    List<KeyValue> key_value_metadata = parquetMetadata.getKey_value_metadata();
    if (key_value_metadata != null) {
      for (KeyValue keyValue : key_value_metadata) {
        keyValueMetaData.put(keyValue.key, keyValue.value);
      }
    }
    return new RedelmMetaData(fileMetadata, blocks, keyValueMetaData);
  }

  MessageType fromParquetSchema(List<SchemaElement> schema) {
    Iterator<SchemaElement> iterator = schema.iterator();
    SchemaElement root = iterator.next();
    return new MessageType(root.getName(), convertChildren(iterator, root.getNum_children()));
  }

  private redelm.schema.Type[] convertChildren(Iterator<SchemaElement> schema, int childrenCount) {
    redelm.schema.Type[] result = new redelm.schema.Type[childrenCount];
    for (int i = 0; i < result.length; i++) {
      SchemaElement schemaElement = schema.next();
      if ((!schemaElement.isSetType() && !schemaElement.isSetNum_children())
          || (schemaElement.isSetType() && schemaElement.isSetNum_children())) {
        throw new RuntimeException("bad element " + schemaElement);
      }
      Repetition repetition = fromParquetRepetition(schemaElement.getRepetition_type());
      String name = schemaElement.getName();
      if (schemaElement.type != null) {
        result[i] = new PrimitiveType(
            repetition,
            getPrimitive(schemaElement.getType()),
            name);
      } else {
        result[i] = new GroupType(
            repetition,
            name,
            convertChildren(schema, schemaElement.getNum_children()));
      }
    }
    return result;
  }

  private Repetition fromParquetRepetition(FieldRepetitionType repetition) {
    switch (repetition) {
    case REQUIRED: return Repetition.REQUIRED;
    case OPTIONAL: return Repetition.OPTIONAL;
    case REPEATED: return Repetition.REPEATED;
    }
    throw new RuntimeException("unknown repetition: " + repetition);
  }

  public void writePageHeader(PageHeader pageHeader, OutputStream to) throws IOException {
    write(pageHeader, to);
  }

  public PageHeader readPageHeader(InputStream from) throws IOException {
    return read(from, new PageHeader());
  }

  public void writeFileMetaData(parquet.format.FileMetaData fileMetadata, OutputStream to) throws IOException {
    write(fileMetadata, to);
  }

  public parquet.format.FileMetaData readFileMetaData(InputStream from) throws IOException {
    return read(from, new parquet.format.FileMetaData());
  }

  public String toString(TBase<?, ?> tbase) {
    return tbase.toString();
//    TMemoryBuffer trans = new TMemoryBuffer(1024);
//    try {
//      TSimpleJSONProtocol jsonProt = new TSimpleJSONProtocol(trans);
//      tbase.write(jsonProt);
//      return trans.toString("UTF-8");
//    } catch (Exception e) { // TODO: cleanup exceptions
//      throw new RuntimeException(e);
//    }
  }

  private TCompactProtocol protocol(OutputStream to) {
    return new TCompactProtocol(new TIOStreamTransport(to));
  }

  private TCompactProtocol protocol(InputStream from) {
    return new TCompactProtocol(new TIOStreamTransport(from));
  }

  private <T extends TBase<?,?>> T read(InputStream from, T tbase)
      throws IOException {
    try {
      tbase.read(protocol(from));
      return tbase;
    } catch (TException e) {
      throw new IOException("can not read " + tbase.getClass() + ": " + e.getMessage(), e);
    }
  }

  private void write(TBase<?, ?> tbase, OutputStream to)
      throws IOException {
    try {
      tbase.write(protocol(to));
    } catch (TException e) {
      throw new IOException("can not write " + tbase, e);
    }
  }

}
