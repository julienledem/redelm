package redelm.column.primitive;

import java.io.DataOutput;
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
  private int bitsPerValue;
  private BitWriter bitWriter = new BitWriter();

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
    byte[] buf = bitWriter.finish();
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
    isFirst = true;
    bitWriter.reset();
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
      bitWriter.writeBit(true);
      bitWriter.writeBits(currentValue, bitsPerValue);
      bitWriter.writeUnsignedVarint(currentValueCt);
    } else {
      for (int i = 0; i < currentValueCt; i++) {
        bitWriter.writeBit(false);
        bitWriter.writeBits(currentValue, bitsPerValue);
      }
    }
  }

  private void newCurrentValue(int val) {
    currentValue = val;
    currentValueCt = 1;
    currentValueIsRepeated = false;
  }
}