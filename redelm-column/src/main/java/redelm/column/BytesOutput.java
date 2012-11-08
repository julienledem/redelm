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
package redelm.column;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;

//TODO compare to implementation in DataOutputStream, just in case
public abstract class BytesOutput implements DataOutput {

  public abstract void write(byte[] bytes, int index, int length) throws IOException;

  @Override
  public void write(int b) throws IOException {
    write(new byte[] { (byte)b }, 0, 1);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void writeBoolean(boolean v) throws IOException {
      write(v ? 1 : 0);
  }

  @Override
  public void writeByte(int v) throws IOException {
    write(v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    write((byte)(0xff & (v >> 8)));
    write((byte)(0xff & v));
  }

  @Override
  public void writeChar(int v) throws IOException {
    write((byte)(0xff & (v >> 8)));
    write((byte)(0xff & v));
  }

  @Override
  public void writeInt(int v) throws IOException {
    write((byte)(0xff & (v >> 24)));
    write((byte)(0xff & (v >> 16)));
    write((byte)(0xff & (v >> 8)));
    write((byte)(0xff & v));
  }

  @Override
  public void writeLong(long v) throws IOException {
    write((byte)(0xff & (v >> 56)));
    write((byte)(0xff & (v >> 48)));
    write((byte)(0xff & (v >> 40)));
    write((byte)(0xff & (v >> 32)));
    write((byte)(0xff & (v >> 24)));
    write((byte)(0xff & (v >> 16)));
    write((byte)(0xff & (v >> 8)));
    write((byte)(0xff & v));
  }

  @Override
  public void writeFloat(float v) throws IOException {
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public void writeDouble(double v) throws IOException {
    writeLong(Double.doubleToLongBits(v));
  }

  @Override
  public void writeBytes(String s) throws IOException {
    int len = s.length();
    byte[] buf = new byte[len];
    for (int i = 0; i < len; i++) {
      buf[i] = (byte)s.charAt(i);
    }
    write(buf, 0, buf.length);
  }

  @Override
  public void writeChars(String s) throws IOException {
    int len = s.length();
    byte[] buf = new byte[2 * len];
    int bufIdx = 0;
    for (int i = 0; i < len; i++) {
      char v = s.charAt(i);
      buf[bufIdx++] = (byte)(0xff & (v >> 8));
      buf[bufIdx++] = (byte)(0xff & v);
    }
    write(buf, 0, bufIdx);
  }

  //TODO this is different from how the DataOutput interface specifies it should work
  //TODO bring it to parity
  @Override
  public void writeUTF(String s) throws IOException {
    write(Base64.encodeBase64(s.getBytes()));
  }
}