package org.apache.pinot.segment.local.io.writer.impl;

import java.nio.ByteBuffer;

public class BitBufferWriter {
  private static final int DEFAULT_BUFFER_SIZE = 4096;

  private ByteBuffer buffer;
  private byte currentByte;
  private int remainingBitsInByte = 8;

  /**
   * Constructs a BitBufferWriter with a default buffer size of 4096 bytes.
   */
  public BitBufferWriter() {
    this(DEFAULT_BUFFER_SIZE);
  }

  /**
   * Constructs a BitBufferWriter with a specified buffer size.
   * Recommended to use values that are multiples of 4096.
   *
   * @param bufferSize The size of the buffer to allocate.
   */
  public BitBufferWriter(int bufferSize) {
    buffer = ByteBuffer.allocateDirect(bufferSize);
    currentByte = buffer.get(buffer.position());
  }

  /**
   * Expands the buffer's capacity by doubling its current size.
   */
  private void expandBuffer() {
    ByteBuffer expandedBuffer = ByteBuffer.allocateDirect(buffer.capacity() * 2);
    buffer.flip();
    expandedBuffer.put(buffer);
    expandedBuffer.position(buffer.capacity());
    buffer = expandedBuffer;
  }

  /**
   * Writes the current byte to the buffer if it's fully packed with bits,
   * and prepares for the next byte if necessary.
   */
  private void flushCurrentByte() {
    if (remainingBitsInByte == 0) {
      buffer.put(currentByte);
      if (!buffer.hasRemaining()) {
        expandBuffer();
      }
      currentByte = buffer.get(buffer.position());
      remainingBitsInByte = 8;
    }
  }

  public void writeBit() {
    currentByte |= (1 << (remainingBitsInByte - 1));
    remainingBitsInByte--;
    flushCurrentByte();
  }

  public void skipBit() {
    remainingBitsInByte--;
    flushCurrentByte();
  }

  /**
   * Writes a specified number of bits from a long value into the buffer.
   *
   * @param value The value to write.
   * @param bitCount The number of bits to write from the value.
   */
  public void writeBits(long value, int bitCount) {
    while (bitCount > 0) {
      int bitsToWrite = Math.min(remainingBitsInByte, bitCount);
      long bitShift = bitCount - bitsToWrite;
      currentByte |= (byte) ((value >> bitShift) & ((1 << bitsToWrite) - 1));
      remainingBitsInByte -= bitsToWrite;
      bitCount -= bitsToWrite;
      flushCurrentByte();
    }
  }

  /**
   * Writes a specified number of bits from an int value into the buffer.
   *
   * @param value The value to write.
   * @param bitCount The number of bits to write from the value.
   */
  public void writeBits(int value, int bitCount) {
    while (bitCount > 0) {
      int bitsToWrite = Math.min(remainingBitsInByte, bitCount);
      int bitShift = bitCount - bitsToWrite;
      currentByte |= (byte) ((value >> bitShift) & ((1 << bitsToWrite) - 1));
      remainingBitsInByte -= bitsToWrite;
      bitCount -= bitsToWrite;
      flushCurrentByte();
    }
  }

  /**
   * Forces any remaining bits to be written to the buffer.
   */
  public void flush() {
    remainingBitsInByte = 0;
    flushCurrentByte(); // Final write to the buffer.
  }

  public boolean isNotEmpty() {
    return buffer.position() > 0 || remainingBitsInByte < 8;
  }

  public void clear() {
    buffer.clear();
    currentByte = buffer.get(buffer.position());
    remainingBitsInByte = 8;
  }

  public int getBufferSize() {
    return remainingBitsInByte == 8 ? buffer.position() : buffer.position() + 1;
  }

  /**
   * Retrieves the underlying buffer containing the packed bits.
   *
   * @return The ByteBuffer containing the bits.
   */
  public ByteBuffer getBuffer() {
    return this.buffer;
  }
}
