package redelm.column.primitive;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

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
  private boolean currentValueIsRepeated = false;
  private int shouldRepeatThreshold = 0;
  private ByteArrayOutputStream bytes = new ByteArrayOutputStream();
  private DataOutputStream bytesData = new DataOutputStream(bytes);
  private int currentByte = 0;
  private int currentBytePosition = 0;
  private int bitsPerValue;
  private boolean isFirst = true;
  private static final int[] byteToTrueMask = new int[8];
  static {
    int currentMask = 1;
    for (int i = 0; i < byteToTrueMask.length; i++) {
      byteToTrueMask[i] = currentMask;
      currentMask <<= 1;
    }
  }

  public BoundedIntColumnWriter(int bound) {
    if (bound == 0) {
      throw new RuntimeException("Value bound cannot be 0. Use DevNullColumnWriter instead.");
    }
    bitsPerValue = (int)Math.ceil(Math.log(bound + 1)/Math.log(2));
    shouldRepeatThreshold = (bitsPerValue + 9)/(1 + bitsPerValue);
  }

  @Override
  public int getMemSize() {
    throw new RuntimeException("Need to implement getMemSize()");
  }

  // This assumes that the full state must be serialized, since there is no close method
  @Override
  public void writeData(DataOutput out) throws IOException {
    serializeCurrentValue();
    writeCachedByte();
    bytesData.close();
    byte[] buf = bytes.toByteArray();
    // We serialize the length so that on deserialization we can
    // deserialize as we go, instead of having to load everything
    // into memory
    Varint.writeSignedVarInt(buf.length, out);
    out.write(buf);
    reset();
  }

  @Override
  public void reset() {
    currentValue = -1;
    currentValueCt = -1;
    currentValueIsRepeated = false;
    bytes = new ByteArrayOutputStream();
    bytesData = new DataOutputStream(bytes);
    isFirst = true;
    currentByte = 0;
    currentBytePosition = 0;
  }

  @Override
  public void writeInteger(int val) {
    if (currentValue == val) {
      currentValueCt++;
      if (!currentValueIsRepeated && currentValueCt >= shouldRepeatThreshold) {
        currentValueIsRepeated = true;
      }
    } else {
      try {
        if (!isFirst) {
          serializeCurrentValue();
        } else {
          isFirst = false;
        }
      } catch (IOException e) {
        throw new RuntimeException("Error serializing current value: " + currentValue, e);
      }
      newCurrentValue(val);
    }
  }

  private void serializeCurrentValue() throws IOException {
    if (currentValueIsRepeated) {
      writeBit(true);
      writeBoundedInt(currentValue);
      writeSignedVarInt(currentValueCt);
    } else {
      for (int i = 0; i < currentValueCt; i++) {
        writeBit(false);
        writeBoundedInt(currentValue);
      }
    }
  }

  //consider doing "unsigned" values and just not allowing 2^32. That's unlikely and it will compress things much more
  private void writeSignedVarInt(int value) throws IOException {
    value = (value << 1) ^ (value >> 31);
    while ((value & 0xFFFFFF80) != 0L) {
      writeLocalByte((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    writeLocalByte(value & 0x7F);
  }

  private void writeLocalByte(int val) throws IOException {
    currentByte |= ((val & 0xFF) << currentBytePosition);
    bytesData.writeByte(currentByte);
    currentByte >>>= 8;
  }

  private void writeBit(boolean bit) throws IOException {
    currentByte = setBytePosition(currentByte, currentBytePosition++, bit);
    if (currentBytePosition == 8) {
      bytesData.write(currentByte);
      currentByte = 0;
      currentBytePosition = 0;
    }
  }

  private void writeCachedByte() throws IOException {
    if (currentBytePosition > 0) {
      bytesData.write(currentByte);
    }
  }

  // The expectation is that the bounded value will be small. If it is > 2^24, then there are
  // going to be issues here. The schema require to make such a large bound, however, is
  // impractical.
  private void writeBoundedInt(int val) throws IOException {
    val <<= currentBytePosition;
    int upperByte = currentBytePosition + bitsPerValue;
    currentByte |= val;
    while (upperByte >= 8) {
      bytesData.write(currentByte); //this only writes the lowest byte
      upperByte -= 8;
      currentByte >>>= 8;
    }
    currentBytePosition = (currentBytePosition + bitsPerValue) % 8;
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