package redelm.column.primitive;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BitWriter {
  private ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private int currentByte = 0;
  private int currentBytePosition = 0;
  private static final int[] byteToTrueMask = new int[8];
  private static final int[] byteToFalseMask = new int[8];
  static {
    int currentMask = 1;
    for (int i = 0; i < byteToTrueMask.length; i++) {
      byteToTrueMask[i] = currentMask;
      byteToFalseMask[i] = ~currentMask;
      currentMask <<= 1;
    }
  }

  public void writeBit(boolean bit) {
    currentByte = setBytePosition(currentByte, currentBytePosition++, bit);
    if (currentBytePosition == 8) {
      baos.write(currentByte);
      currentByte = 0;
      currentBytePosition = 0;
    }
  }

  public void writeByte(int val) {
    currentByte |= ((val & 0xFF) << currentBytePosition);
    baos.write(currentByte);
    currentByte >>>= 8;
  }

  public void writeBits(int val, int bitsToWrite) {
    val <<= currentBytePosition;
    int upperByte = currentBytePosition + bitsToWrite;
    currentByte |= val;
    while (upperByte >= 8) {
      baos.write(currentByte); //this only writes the lowest byte
      upperByte -= 8;
      currentByte >>>= 8;
    }
    currentBytePosition = (currentBytePosition + bitsToWrite) % 8;
  }

  public byte[] finish() {
    if (currentBytePosition > 0) {
      baos.write(currentByte);
    }
    try {
      baos.flush();
    } catch (IOException e) {
      // This shouldn't be possible as ByteArrayOutputStream
      // uses OutputStream's flush, which is a noop
      throw new RuntimeException(e);
    }
    byte[] buf = baos.toByteArray();
    reset();
    return buf;
  }

  public void reset() {
    baos = new ByteArrayOutputStream();
    currentByte = 0;
    currentBytePosition = 0;
  }

  public static int setBytePosition(int currentByte, int currentBytePosition, boolean bit) {
    if (bit) {
      currentByte |= byteToTrueMask[currentBytePosition];
    } else {
      currentByte &= byteToFalseMask[currentBytePosition];
    }
    return currentByte;
  }
}
