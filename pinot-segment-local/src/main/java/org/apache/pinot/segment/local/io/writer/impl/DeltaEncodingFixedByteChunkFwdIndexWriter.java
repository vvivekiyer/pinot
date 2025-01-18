package org.apache.pinot.segment.local.io.writer.impl;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.pinot.segment.local.io.compression.ChunkCompressorFactory;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeltaEncodingFixedByteChunkFwdIndexWriter implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseChunkForwardIndexWriter.class);

  private int _sizeOfEntryInBits;

  protected final FileChannel _dataFile;
  protected ByteBuffer _header;
  protected final BitBufferWriter _chunkBuffer;
  protected final ByteBuffer _compressedBuffer;
  protected final ChunkCompressor _chunkCompressor;

  protected int _chunkSize;
  protected long _dataOffset;

  protected long _minValue;

  private final int _headerEntryChunkOffsetSize;


  public DeltaEncodingFixedByteChunkFwdIndexWriter(File file, ChunkCompressionType compressionType, int totalDocs,
      int numDocsPerChunk, int writerVersion, Comparable<?> minValue, Comparable<?> maxValue)
      throws IOException {
    _sizeOfEntryInBits = calculateSizeOfEntry(minValue, maxValue);
    _chunkSize = ((numDocsPerChunk * _sizeOfEntryInBits) / 8);
    if ((numDocsPerChunk * _sizeOfEntryInBits) % 8 != 0) {
      _chunkSize += 1; // Add 1 byte if it's not byte-aligned
    }
    Preconditions.checkArgument(_chunkSize <= Integer.MAX_VALUE, "Chunk size limited to 2GB");

    _chunkCompressor = ChunkCompressorFactory.getCompressor(compressionType);
    // TODO(Vivek): Check if this is needed.
    _headerEntryChunkOffsetSize = Long.BYTES;

    _minValue = (Long) minValue;
    _dataOffset = writeHeader(compressionType, totalDocs, numDocsPerChunk, writerVersion, (Long) minValue);
    _chunkBuffer = new BitBufferWriter(_chunkSize);
    int maxCompressedChunkSize = _chunkCompressor.maxCompressedSize(_chunkSize); // may exceed original chunk size
    _compressedBuffer = ByteBuffer.allocateDirect(maxCompressedChunkSize);
    _dataFile = new RandomAccessFile(file, "rw").getChannel();
  }

  @Override
  public void close()
      throws IOException {

    // Write the chunk if it is non-empty.
    if (_chunkBuffer.isNotEmpty()) {
      writeChunk();
    }

    // Write the header and close the file.
    _header.flip();
    _dataFile.write(_header, 0);
    _dataFile.close();
    _chunkCompressor.close();
  }




  public void putInt(int value) {
    int delta = value - (int) _minValue;

    // Validate if _bitsNeededPerEntry is sufficient to represent the difference
    if (delta < 0 || delta >= (1 << _sizeOfEntryInBits)) {
      throw new IllegalArgumentException("Delta is too large to fit in the specified bit width");
    }

    _chunkBuffer.writeBits(delta, _sizeOfEntryInBits);
    flushChunkIfNeeded();
  }


  public void putLong(long value) {
    long delta = value -  _minValue;

    // Validate if _bitsNeededPerEntry is sufficient to represent the difference
    if (delta < 0 || delta >= (1 << _sizeOfEntryInBits)) {
      throw new IllegalArgumentException("Delta is too large to fit in the specified bit width");
    }

    _chunkBuffer.writeBits(delta, _sizeOfEntryInBits);
    flushChunkIfNeeded();
  }

  private void flushChunkIfNeeded() {
    // If buffer filled, then compress and write to file.
    if (_chunkBuffer.getBufferSize() == _chunkSize) {
      writeChunk();
    }
  }


  /**
   * Helper method to write header information.
   *
   * @param compressionType Compression type for the data
   * @param totalDocs Total number of records
   * @param numDocsPerChunk Number of documents per chunk
   * @param version Version of file
   * @param minValue Minimum value in the column
   * @return Size of header
   */
  private int writeHeader(ChunkCompressionType compressionType, int totalDocs, int numDocsPerChunk, int version, long minValue) {
    int numChunks = (totalDocs + numDocsPerChunk - 1) / numDocsPerChunk;
    int headerSize = (8 * Integer.BYTES) + Long.BYTES + (numChunks * _headerEntryChunkOffsetSize);

    _header = ByteBuffer.allocateDirect(headerSize);

    int offset = 0;
    _header.putInt(version);
    offset += Integer.BYTES;

    _header.putInt(numChunks);
    offset += Integer.BYTES;

    _header.putInt(numDocsPerChunk);
    offset += Integer.BYTES;

    _header.putInt(_sizeOfEntryInBits);
    offset += Integer.BYTES;

    // Write total number of docs.
    _header.putInt(totalDocs);
    offset += Integer.BYTES;

    // Write the compressor type
    _header.putInt(compressionType.getValue());
    offset += Integer.BYTES;

    // Write the special encoding type (eg: Delta, Frame of Reference)
    // TODO(Vivek): Change this to include proper enum.
    _header.putInt(1);
    offset += Integer.BYTES;

    _header.putLong(minValue);
    offset += Long.BYTES;

    // Start of chunk offsets.
    int dataHeaderStart = offset + Integer.BYTES;
    _header.putInt(dataHeaderStart);

    return headerSize;
  }

  protected void writeChunk() {
    int sizeToWrite;
    _chunkBuffer.flush();
    _chunkBuffer.getBuffer().flip();

    try {
      sizeToWrite = _chunkCompressor.compress(_chunkBuffer.getBuffer(), _compressedBuffer);
      _dataFile.write(_compressedBuffer, _dataOffset);
      _compressedBuffer.clear();
    } catch (IOException e) {
      LOGGER.error("Exception caught while compressing/writing data chunk", e);
      throw new RuntimeException(e);
    }

    if (_headerEntryChunkOffsetSize == Integer.BYTES) {
      Preconditions.checkState(_dataOffset <= Integer.MAX_VALUE, "Integer overflow detected. "
          + "Try to use raw version 3 or 4, reduce targetDocsPerChunk or targetMaxChunkSize");
      _header.putInt((int) _dataOffset);
    } else if (_headerEntryChunkOffsetSize == Long.BYTES) {
      _header.putLong(_dataOffset);
    }

    _dataOffset += sizeToWrite;
    _chunkBuffer.clear();
  }


  private static int calculateSizeOfEntry(Comparable<?> minValue, Comparable<?> maxValue) {
    // Determine the range and calculate bits required for delta encoding
    if (minValue instanceof Integer && maxValue instanceof Integer) {
      int min = (Integer) minValue;
      int max = (Integer) maxValue;
      int absDiff = max - min;
      return bitsRequired(absDiff);
    } else if (minValue instanceof Long && maxValue instanceof Long) {
      long min = (Long) minValue;
      long max = (Long) maxValue;
      long absDiff = max - min;
      return bitsRequired(absDiff);
    } else {
      // TODO(Vivek): Handle float and double.
      throw new IllegalArgumentException("Unsupported data type for delta encoding");
    }
  }


  public static int bitsRequired(long value) {
    if (value < 0) {
      throw new IllegalArgumentException("Value must be non-negative");
    }
    return Long.SIZE - Long.numberOfLeadingZeros(value);
  }

  /**
   * Returns the number of bits required to represent the given non-negative value.
   *
   * @param value the value to analyze (must be non-negative)
   * @return the number of bits required to represent the value
   */
  public static int bitsRequired(int value) {
    if (value < 0) {
      throw new IllegalArgumentException("Value must be non-negative");
    }
    return Integer.SIZE - Integer.numberOfLeadingZeros(value);
  }
}
