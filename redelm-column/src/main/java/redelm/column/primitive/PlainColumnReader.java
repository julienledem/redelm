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
package redelm.column.primitive;

import static redelm.Log.DEBUG;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import redelm.Log;
import redelm.bytes.BytesUtils;
import redelm.bytes.LittleEndianDataInputStream;

/**
 * Plain encoding except for booleans
 *
 * @author Julien Le Dem
 *
 */
public class PlainColumnReader extends PrimitiveColumnReader {
  private static final Log LOG = Log.getLog(PlainColumnReader.class);

  private LittleEndianDataInputStream in;

  @Override
  public float readFloat() {
    try {
      return in.readFloat();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public byte[] readBytes() {
    try {
//      byte[] value = new byte[BytesUtils.readUnsignedVarInt(in)];
      byte[] value = new byte[in.readInt()];
      in.readFully(value);
      return value;
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public double readDouble() {
    try {
      return in.readDouble();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public int readInteger() {
    try {
      return in.readInt();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public long readLong() {
    try {
      return in.readLong();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public int readByte() {
    try {
      return in.read();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  /**
   * {@inheritDoc}
   * @see redelm.column.primitive.PrimitiveColumnReader#initFromPage(byte[], int)
   */
  @Override
  public int initFromPage(long valueCount, byte[] in, int offset) throws IOException {
    if (DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    this.in = new LittleEndianDataInputStream(new ByteArrayInputStream(in, offset, in.length - offset));
    return in.length;
  }

}