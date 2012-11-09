package redelm.column.primitive;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import redelm.utils.Varint;

public class BoundedIntColumnReader extends PrimitiveColumnReader {
  private ByteArrayInputStream bytes;
  private DataInputStream bytesData;
  private int currentValueCt = 0;
  private int currentValue = -1;
  private int bound = 0;
  private boolean currentValueIsRepeated = false;
  private static final int[] byteGetValueMask = new int[8];
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

  @Override
  public int readInteger() {
    try {
      if (currentValueCt > 0) {
        currentValueCt--;
        return currentValue;
      }
      currentValueIsRepeated = readBit();
      if (currentValueIsRepeated) {
        readBoundedInt(bound);
        currentValueCt = Varint.readSignedVarInt(bytesData);
      } else {
        readBoundedInt(bound);
        currentValueCt = 1;
      }
      return 0;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private int currentByte = 0;
  private int currentPosition = 8;

  private boolean readBit() throws IOException {
    if (currentPosition == 8) {
      currentByte = (byte)bytesData.readByte();
      currentPosition = 0;
    }
    return getBytePosition(currentByte, currentPosition++);
  }

  private void readBoundedInt(int bound) throws IOException {
    currentValue = currentByte >> currentPosition;
    int toShift = currentPosition;
    while (bound >= 8) {
      currentByte = bytesData.read();
      currentValue |= currentByte << toShift;
      toShift += 8;
      bound -= 8;
    }
    currentValue &= readMask[bound + currentPosition];
    currentPosition = (bound + currentPosition) % 8;
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
    bound = Varint.readSignedVarInt(in);
    int totalBytes = Varint.readSignedVarInt(in);
    if (totalBytes > 0) {
      byte[] buf = new byte[totalBytes];
      in.readFully(buf);
      bytes = new ByteArrayInputStream(buf);
      bytesData = new DataInputStream(bytes);
    }
    currentPosition = 8;
  }

  public int getBound() {
    return bound;
  }
}
