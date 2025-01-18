package org.apache.pinot.segment.local.io.writer.impl;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.segment.local.io.compression.ChunkCompressorFactory;
import org.apache.pinot.segment.local.io.util.PinotByteBufferBitSet;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FrameOfReferenceChunkFwdIndexWriter implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FrameOfReferenceChunkFwdIndexWriter.class);

  public static final byte[] MAGIC_BYTES = "FOR.v1".getBytes(StandardCharsets.UTF_8);

  private int _sizeOfEntryInBits;

  protected final FileChannel _dataFile;
  protected ByteBuffer _header;
  protected final ByteBuffer _chunkBuffer;
  protected final ByteBuffer _compressedBuffer;
  protected final ChunkCompressor _chunkCompressor;

  protected final PinotByteBufferBitSet _pinotByteBufferBitSet;

  protected int _chunkSize;
  protected long _dataOffset;

  protected long _minValue;

  private final int _headerEntryChunkOffsetSize;

  private int _chunkIndex;


  public FrameOfReferenceChunkFwdIndexWriter(File file, ChunkCompressionType compressionType, int totalDocs,
      int numDocsPerChunk, Comparable<?> minValue, Comparable<?> maxValue)
      throws IOException {
    _sizeOfEntryInBits = calculateSizeOfEntry(minValue, maxValue);

    // Each chunk might contain some unused bits at the end. We need to round up to the nearest byte.
    _chunkSize = (numDocsPerChunk * _sizeOfEntryInBits + Byte.SIZE - 1) / Byte.SIZE;
    Preconditions.checkArgument(_chunkSize <= Integer.MAX_VALUE, "Chunk size limited to 2GB");

    _chunkCompressor = ChunkCompressorFactory.getCompressor(compressionType);

    // After the column header, we store the data header. It is essentially the start offset for every chunk.
    _headerEntryChunkOffsetSize = Long.BYTES;
    if (minValue instanceof Number) {
      _minValue = ((Number) minValue).longValue();
    }

    _dataOffset = writeHeader(compressionType, totalDocs, numDocsPerChunk, _minValue);

    int maxCompressedChunkSize = _chunkCompressor.maxCompressedSize(_chunkSize); // may exceed original chunk size
    _compressedBuffer = ByteBuffer.allocateDirect(maxCompressedChunkSize);
    _chunkBuffer = ByteBuffer.allocateDirect(_chunkSize);
    _pinotByteBufferBitSet = new PinotByteBufferBitSet(_chunkBuffer);

    // TODO(Vivek): Delete
    _dataFile = new RandomAccessFile(file, "rw").getChannel();

    _chunkIndex = 0;
  }

  @Override
  public void close()
      throws IOException {

    // Write the chunk if it is non-empty.
    if (_chunkBuffer.position() > 0) {
      writeChunk();
    }

    // Write the header and close the file.
    _header.flip();
    _dataFile.write(_header, 0);
    _dataFile.close();
    _chunkCompressor.close();
    _pinotByteBufferBitSet.close();
  }

  public void putInt(int value) {
    int delta = value - (int) _minValue;

    // Validate if _bitsNeededPerEntry is sufficient to represent the difference
    if (delta < 0 || delta >= (1 << _sizeOfEntryInBits)) {
      throw new IllegalArgumentException("Delta is too large to fit in the specified bit width");
    }

    _pinotByteBufferBitSet.writeInt(_chunkIndex, _sizeOfEntryInBits, delta);
    _chunkIndex++;
    flushChunkIfNeeded();
  }


  public void putLong(long value) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private void flushChunkIfNeeded() {
    long chunkOffset = (_chunkIndex * _sizeOfEntryInBits + _sizeOfEntryInBits + Byte.SIZE - 1)  / Byte.SIZE;

    // If buffer filled, then compress and write to file.
    if (chunkOffset > _chunkSize) {
      writeChunk();
    }
  }


  /**
   * Helper method to write header information.
   *
   * @param compressionType Compression type for the data
   * @param totalDocs Total number of records
   * @param numDocsPerChunk Number of documents per chunk
   * @param minValue Minimum value in the column
   * @return Size of header
   */
  private int writeHeader(ChunkCompressionType compressionType, int totalDocs, int numDocsPerChunk, long minValue) {
    int numChunks = (totalDocs + numDocsPerChunk - 1) / numDocsPerChunk;
    int headerSize = MAGIC_BYTES.length + (6 * Integer.BYTES) + Long.BYTES  + (numChunks * _headerEntryChunkOffsetSize);

    _header = ByteBuffer.allocateDirect(headerSize);

    int offset = 0;
    _header.put(MAGIC_BYTES);
    offset += MAGIC_BYTES.length;

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

    _header.putLong(minValue);
    offset += Long.BYTES;

    // Start of chunk offsets.
    int dataHeaderStart = offset + Integer.BYTES;
    _header.putInt(dataHeaderStart);

    return headerSize;
  }

  protected void writeChunk() {
    int sizeToWrite;

    try {
      sizeToWrite = _chunkCompressor.compress(_chunkBuffer, _compressedBuffer);
      _dataFile.write(_compressedBuffer, _dataOffset);
      _compressedBuffer.clear();
    } catch (IOException e) {
      LOGGER.error("Exception caught while compressing/writing data chunk", e);
      throw new RuntimeException(e);
    }


    _header.putLong(_dataOffset);
    _dataOffset += sizeToWrite;
    _chunkIndex = 0;
    _chunkBuffer.clear();
  }


  private static int calculateSizeOfEntry(Comparable<?> minValue, Comparable<?> maxValue) {
    // Determine the range and calculate bits required for delta encoding
    if (minValue instanceof Integer && maxValue instanceof Integer) {
      int min = (Integer) minValue;
      int max = (Integer) maxValue;
      int absDiff = max - min;
      return PinotDataBitSet.getNumBitsPerValue(absDiff);
    } else {
      // TODO(Vivek): Handle float and double.
      throw new IllegalArgumentException("Unsupported data type for delta encoding");
    }
  }
}
