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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class RedelmInputSplit extends InputSplit implements Serializable, Writable {
  private static final long serialVersionUID = 1L;

  private String path;
  private long start;
  private long length;
  private String[] hosts;
  private BlockMetaData block;
  private String schemaString;
  private String pigSchemaString;
  private String codecClassName;

  public RedelmInputSplit() {
  }

  public RedelmInputSplit(Path path, long start, long length, String[] hosts, BlockMetaData block, String schemaString, String pigSchemaString, String codecClassName) {
    this.path = path.toUri().toString();
    this.start = start;
    this.length = length;
    this.hosts = hosts;
    this.block = block;
    this.schemaString = schemaString;
    this.codecClassName = codecClassName;
    if (pigSchemaString == null) {
      throw new NullPointerException("pigSchemaString is null");
    }
    this.pigSchemaString = pigSchemaString;
  }

  public BlockMetaData getBlock() {
    return block;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return length;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return hosts;
  }

  public long getStart() {
    return start;
  }

  public Path getPath() {
    try {
      return new Path(new URI(path));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int l = in.readInt();
    byte[] b = new byte[l];
    in.readFully(b);
    RedelmInputSplit other;
    try {
      other = (RedelmInputSplit)
          new ObjectInputStream(new ByteArrayInputStream(b))
        .readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("wrong class serialized", e);
    }
    this.path = other.path;
    this.start = other.start;
    this.length = other.length;
    this.hosts = other.hosts;
    this.block = other.block;
    this.schemaString = other.schemaString;
    this.pigSchemaString = other.pigSchemaString;
    this.codecClassName = other.codecClassName;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new ObjectOutputStream(baos).writeObject(this);
    byte[] b = baos.toByteArray();
    out.writeInt(b.length);
    out.write(b);
  }

  public String getSchema() {
    return schemaString;
  }

  public String getPigSchema() {
    return pigSchemaString;
  }

  public String getCodecClassName() {
    return codecClassName;
  };

  public String toString() {
    return this.getClass().getSimpleName() + "{" +
           "part: " + path
        + " start: " + start
        + " length: " + length
        + " hosts: " + hosts
        + " block: " + block
        + " schema: " + schemaString
        + " pigSchema: " + pigSchemaString
        + "}";
  }
}
