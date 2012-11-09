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
import java.io.OutputStream;

import org.apache.commons.codec.binary.Base64;

public abstract class BytesOutput extends OutputStream implements DataOutput {
  public abstract void write(byte[] bytes, int index, int length) throws IOException;

  @Override
  public void write(int b) throws IOException {
    write(new byte[] { (byte)b }, 0, 1);
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
    byte[] buf = new byte[4];
    buf[0] = (byte)(0xff & (v >> 8));
    buf[1] = (byte)(0xff & v);
    write(buf, 0, 2);
  }

  @Override
  public void writeChar(int v) throws IOException {
    writeShort(v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    byte[] buf = new byte[4];
    buf[0] = (byte)(0xff & (v >> 24));
    buf[1] = (byte)(0xff & (v >> 16));
    buf[2] = (byte)(0xff & (v >> 8));
    buf[3] = (byte)(0xff & v);
    write(buf, 0, 4);
  }

  @Override
  public void writeLong(long v) throws IOException {
    byte[] buf = new byte[8];
    buf[0] = (byte)(0xff & (v >> 56));
    buf[1] = (byte)(0xff & (v >> 48));
    buf[2] = (byte)(0xff & (v >> 40));
    buf[3] = (byte)(0xff & (v >> 32));
    buf[4] = (byte)(0xff & (v >> 24));
    buf[5] = (byte)(0xff & (v >> 16));
    buf[6] = (byte)(0xff & (v >> 8));
    buf[7] = (byte)(0xff & v);
    write(buf, 0, 8);
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
    byte[] bytes = Base64.encodeBase64(s.getBytes());
    writeInt(bytes.length);
    write(bytes);
  }
}