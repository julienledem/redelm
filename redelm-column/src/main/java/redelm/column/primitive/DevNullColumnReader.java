package redelm.column.primitive;

import java.io.DataInputStream;
import java.io.IOException;

public class DevNullColumnReader extends PrimitiveColumnReader {
  private boolean defaultBoolean = false;
  private int defaultInt = 0;
  private long defaultLong = 0L;
  private byte defaultByte = 0;
  private float defaultFloat = 0f;
  private double defaultDouble = 0.0;
  private byte[] defaultBytes = new byte[0];
  private String defaultString = "";

  public void setDefaultBoolean(boolean defaultBoolean) {
    this.defaultBoolean = defaultBoolean;
  }

  public void setDefaultInteger(int defaultInt) {
    this.defaultInt = defaultInt;
  }

  public void setDefaultLong(long defaultLong) {
    this.defaultLong = defaultLong;
  }

  public void setDefaultFloat(float defaultFloat) {
    this.defaultFloat = defaultFloat;
  }

  public void setDefaultDouble(double defaultDouble) {
    this.defaultDouble = defaultDouble;
  }

  public void setDefaultByte(byte defaultByte) {
    this.defaultByte = defaultByte;
  }

  public void setDefaultBytes(byte[] defaultBytes) {
    this.defaultBytes = defaultBytes;
  }

  public void setDefaultString(String defaultString) {
    this.defaultString = defaultString;
  }

  public boolean readBoolean() {
    return defaultBoolean;
  }

  public int readByte() {
    return defaultByte;
  }

  public float readFloat() {
    return defaultFloat;
  }

  public byte[] readBytes() {
    return defaultBytes;
  }

  public String readString() {
    return defaultString;
  }

  public double readDouble() {
    return defaultDouble;
  }

  public int readInteger() {
    return defaultInt;
  }

  public long readLong() {
    return defaultLong;
  }

  @Override
  public void readStripe(DataInputStream in) throws IOException {
  }
}