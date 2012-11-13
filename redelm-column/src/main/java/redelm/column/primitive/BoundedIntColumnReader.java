package redelm.column.primitive;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import redelm.utils.Varint;

public class BoundedIntColumnReader extends PrimitiveColumnReader {
  private ByteArrayInputStream bytes;
  private DataInputStream bytesData;
  private int currentValueCt = 0;
  private int currentValue = 0;
  private static final int[] byteGetValueMask = new int[8];
  private int bitsPerValue = 0;
  static {
    int currentMask = 1;
    for (int i = 0; i < byteGetValueMask.length; i++) {
      byteGetValueMask[i] = currentMask;
      currentMask <<= 1;
    }
  }
  private static final int[] readMask = new int[32];
  static {
    int currentMask = 0;
    for (int i = 0; i < readMask.length; i++) {
      readMask[i] = currentMask;
      currentMask <<= 1;
      currentMask += 1;
    }
  }

  public BoundedIntColumnReader(int bound) {
    if (bound == 0) {
      throw new RuntimeException("Value bound cannot be 0. Use DevNullColumnReader instead.");
    }
    bitsPerValue = (int)Math.ceil(Math.log(bound + 1)/Math.log(2));
  }

  @Override
  public int readInteger() {
    try {
      if (currentValueCt > 0) {
        currentValueCt--;
        return currentValue;
      }
      if (readBit()) {
        readBoundedInt();
        currentValueCt = readSignedVarInt() - 1;
      } else {
        readBoundedInt();
      }
      return currentValue;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private int readSignedVarInt() throws IOException {
    int value = 0;
    int i = 0;
    int b;
    while (((b = readLocalByte()) & 0x80) != 0) {
        value |= (b & 0x7F) << i;
        i += 7;
        if (i > 35) {
            throw new RuntimeException("Variable length quantity is too long");
        }
    }
    int raw = value | (b << i);
    int temp = (((raw << 31) >> 31) ^ raw) >> 1;
    return temp ^ (raw & (1 << 31));
  }

  private int readLocalByte() throws IOException {
    try {
      currentByte |= (bytesData.readUnsignedByte() << 8);
    } catch (EOFException e) {
      //TODO
    }
    int value = (currentByte >>> currentPosition) & 0xFF;
    currentByte >>>= 8;
    return value;
  }

  private int currentByte = 0;
  private int currentPosition = 8;

  private boolean readBit() throws IOException {
    if (currentPosition == 8) {
      currentByte = bytesData.readUnsignedByte();
      currentPosition = 0;
    }
    return getBytePosition(currentByte, currentPosition++);
  }

  private void readBoundedInt() throws IOException {
    int bits = bitsPerValue + currentPosition;
    currentValue = currentByte >>> currentPosition;
    int toShift = 8 - currentPosition;
    while (bits >= 8) {
      try {
        currentByte = bytesData.readUnsignedByte();
      } catch (EOFException e) {
        //TODO
      }
      currentValue |= currentByte << toShift;
      toShift += 8;
      bits -= 8;
    }
    currentValue &= readMask[bitsPerValue];
    currentPosition = (bitsPerValue + currentPosition) % 8;
  }

  private boolean getBytePosition(int val, int position) {
    return (val & byteGetValueMask[position]) != 0;
  }

  // This forces it to deserialize into memory. If it wanted
  // to, it could just read the bytes (though that number of
  // bytes would have to be serialized). This is the flip-side
  // to BoundedIntColumnWriter.writeData(BytesOutput)
  @Override
  public void readStripe(DataInputStream in) throws IOException {
    int totalBytes = Varint.readSignedVarInt(in);
    if (totalBytes > 0) {
      byte[] buf = new byte[totalBytes];
      in.readFully(buf);
      bytes = new ByteArrayInputStream(buf);
      bytesData = new DataInputStream(bytes);
    }
    currentPosition = 8;
  }
}