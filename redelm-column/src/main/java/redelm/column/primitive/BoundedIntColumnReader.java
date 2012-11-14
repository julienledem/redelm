package redelm.column.primitive;

import java.io.DataInputStream;
import java.io.IOException;

import redelm.utils.Varint;

public class BoundedIntColumnReader extends PrimitiveColumnReader {
  private int currentValueCt = 0;
  private int currentValue = 0;
  private int bitsPerValue = 0;
  private BitReader bitReader = new BitReader();

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
      if (bitReader.readBit()) {
        currentValue = bitReader.readBoundedInt(bitsPerValue);
        currentValueCt = bitReader.readUnsignedVarint() - 1;
      } else {
        currentValue = bitReader.readBoundedInt(bitsPerValue);
      }
      return currentValue;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
      bitReader.prepare(buf);
    }
  }
}