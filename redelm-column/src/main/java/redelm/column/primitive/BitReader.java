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

import java.io.IOException;

public class BitReader {
  private int currentByte = 0;
  private int currentPosition = 8;
  private byte[] buf;
  private int currentBufferPosition = 0;
  private static final int[] byteGetValueMask = new int[8];
  private static final int[] readMask = new int[32];
  private int endBufferPosistion;

  static {
    int currentMask = 1;
    for (int i = 0; i < byteGetValueMask.length; i++) {
      byteGetValueMask[i] = currentMask;
      currentMask <<= 1;
    }
    currentMask = 0;
    for (int i = 0; i < readMask.length; i++) {
      readMask[i] = currentMask;
      currentMask <<= 1;
      currentMask += 1;
    }
  }

  public void prepare(byte[] buf, int offset, int length) {
    this.buf = buf;
    this.endBufferPosistion = offset + length;
    currentByte = 0;
    currentPosition = 8;
    currentBufferPosition = offset;
  }

  public static boolean getBytePosition(int val, int position) {
    return (val & byteGetValueMask[position]) != 0;
  }

  public int readBoundedInt(int bitsPerValue) {
    int bits = bitsPerValue + currentPosition;
    int currentValue = currentByte >>> currentPosition;
    int toShift = 8 - currentPosition;
    while (bits >= 8) {
      currentByte = getNextByte();
      currentValue |= currentByte << toShift;
      toShift += 8;
      bits -= 8;
    }
    currentValue &= readMask[bitsPerValue];
    currentPosition = (bitsPerValue + currentPosition) % 8;
    return currentValue;
  }

  private int getNextByte() {
    if (currentBufferPosition < endBufferPosistion) {
      return buf[currentBufferPosition++] & 0xFF;
    }
    return 0;
  }

  public boolean readBit() throws IOException {
    if (currentPosition == 8) {
      currentByte = getNextByte();
      currentPosition = 0;
    }
    return getBytePosition(currentByte, currentPosition++);
  }

  public int readByte() {
    currentByte |= (getNextByte() << 8);
    int value = (currentByte >>> currentPosition) & 0xFF;
    currentByte >>>= 8;
    return value;
  }

  public int readUnsignedVarint() throws IOException {
    int value = 0;
    int i = 0;
    int b;
    while (((b = readByte()) & 0x80) != 0) {
        value |= (b & 0x7F) << i;
        i += 7;
        if (i > 35) {
            throw new RuntimeException("Variable length quantity is too long");
        }
    }
    return value | (b << i);
  }
}