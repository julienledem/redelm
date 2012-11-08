package redelm.column.primitive;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import redelm.column.BytesOutput;
import redelm.utils.Varint;

/**
 * This is a special ColumnWriter for the case when you need to write
 * integers in a known range. This is intended primarily for use with
 * repetition and definition levels, since the maximum value that will
 * be written is known a priori based on the schema. Assumption is that
 * the values written are between 0 and the bound, inclusive.
 */
//TODO write tests BEFORE finishing!
//TODO make the shell for the reader
public class BoundedIntColumnWriter extends PrimitiveColumnWriter {
  private int currentValue = -1;
  private int currentValueCt = -1;
  int serializedCt = 0;
  private boolean currentValueIsRepeated = false;
  private int shouldRepeatThreshold;
  private ByteArrayOutputStream bytes = new ByteArrayOutputStream();
  private DataOutput bytesData = new DataOutputStream(bytes);
  
  public BoundedIntColumnWriter(int bound) {
    int bitsPerValue = (int)Math.ceil(Math.log(bound));
    shouldRepeatThreshold = (bitsPerValue + 9)/(1 + bitsPerValue);
  }

  @Override
  public int getMemSize() {
    throw new RuntimeException("Need to implement getMemSize()");
  }

  // This assumes that the full state must be serialized, since there is no close method
  @Override
  public void writeData(BytesOutput out) throws IOException {
    serializeCurrentValue();
    out.writeInt(serializedCt);
    
  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub

  }

  @Override
  public void writeInt(int val) {
    if (currentValue == val) {
      currentValueCt++;
      if (!currentValueIsRepeated && currentValueCt >= shouldRepeatThreshold) {
        currentValueIsRepeated = true;
      }
    } else {
      try {
        serializeCurrentValue();
      } catch (IOException e) {
        throw new RuntimeException("Error serializing current value: " + currentValue, e);
      }
      newCurrentValue(val);
    }
  }
  
  private void serializeCurrentValue() throws IOException {
    serializedCt++;
    if (currentValueIsRepeated) {
      writeBit(true);
      writeBoundedInt(currentValue);
      Varint.writeSignedVarInt(currentValueCt, bytesData);
    } else {
      for (int i = 0; i < currentValueCt; i++) {
        writeBit(false);
        writeBoundedInt(currentValue);
      }
    }
  }
  
  private int currentByte = 0;
  private int currentBytePosition = 0;
  private static final int[] byteToTrueMask = new int[8];
  static {
    int currentMask = 1;
    for (int i = 0; i < byteToTrueMask.length; i++) {
      byteToTrueMask[i] = currentMask;
      currentMask <<= 1;
    }
  }
  
  private void writeBit(boolean bit) throws IOException {
    currentByte = setBytePosition(currentByte, currentBytePosition, bit);
    if (currentBytePosition == 8) {
      bytesData.write(currentByte);
      currentByte = 0;
      currentBytePosition = 0;
    }
  }
  
  // The expectation is that the bounded value will be small. If it is > 2^24, then there are
  // going to be issues here. The schema require to make such a large bound, however, is
  // impractical.
  private void writeBoundedInt(int val) throws IOException {
    val <<= currentBytePosition;
    currentByte |= val;
    while (currentBytePosition > 8) {
      bytesData.write(currentByte); //this only writes the lowest byte
      currentBytePosition -= 8;
    }
  }

  // This assumes that the default is all false ie 0
  private int setBytePosition(int currentByte, int currentBytePosition, boolean bit) {
    if (bit) {
      currentByte |= byteToTrueMask[currentBytePosition];
    }
    
    return currentByte;
  }
  
  private void newCurrentValue(int val) {
    currentValue = val;
    currentValueCt = 1;
    currentValueIsRepeated = false;
  }
}