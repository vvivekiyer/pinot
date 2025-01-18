package org.apache.pinot.segment.local.segment.index.forward;

import java.io.File;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.FrameOfReferenceChunkFwdIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBytePower2ChunkSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.FrameOfReferenceChunkFwdIndexReader;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class FrameOfReferenceChunkFwdIndexReaderTest {
  private static final int NUM_VALUES = 1000000;
  private static final int NUM_DOCS_PER_CHUNK = 1000;

  private static final int MAX_VAL = 2147483647;

  private static final int MIN_VAL = 2147483639;
  private static final String TEST_FILE = System.getProperty("java.io.tmpdir") + File.separator + "CompressedFwdIndexTest";

  private static final String TEST_FILE_UNCOMPRESSED = System.getProperty("java.io.tmpdir") + File.separator + "NotCompressedFwdIndexTest";

  private static final Random RANDOM = new Random();

  @DataProvider(name = "combinations")
  public static Object[][] combinations() {
    return Arrays.stream(ChunkCompressionType.values())
        .flatMap(chunkCompressionType -> IntStream.of(2, 3, 4)
            .mapToObj(version -> new Object[]{chunkCompressionType, version}))
        .toArray(Object[][]::new);
  }

  @Test
  public void testInt()
      throws Exception {
    int[] expected = new int[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = RANDOM.nextInt(MAX_VAL - MIN_VAL) + MIN_VAL;
    }

    File compressedFile = new File(TEST_FILE);
    FileUtils.deleteQuietly(compressedFile);

    File uncompressedFile = new File(TEST_FILE_UNCOMPRESSED);
    FileUtils.deleteQuietly(uncompressedFile);

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    try (FrameOfReferenceChunkFwdIndexWriter compressedWriter = new FrameOfReferenceChunkFwdIndexWriter(compressedFile,
        ChunkCompressionType.ZSTANDARD, NUM_VALUES, NUM_DOCS_PER_CHUNK, MIN_VAL, MAX_VAL)) {
      for (int i = 0 ; i < NUM_VALUES; i++) {
        compressedWriter.putInt(expected[i]);
      }
    }

    try (FixedByteChunkForwardIndexWriter uncompressedWriter = new FixedByteChunkForwardIndexWriter(uncompressedFile,
        ChunkCompressionType.ZSTANDARD, NUM_VALUES, NUM_DOCS_PER_CHUNK, Integer.BYTES, 4)) {
      for (int i = 0 ; i < NUM_VALUES; i++) {
        uncompressedWriter.putInt(expected[i]);
      }
    }


    try (ForwardIndexReader<ChunkReaderContext> fourByteOffsetReader = new FrameOfReferenceChunkFwdIndexReader(PinotDataBuffer.mapReadOnlyBigEndianFile(compressedFile), FieldSpec.DataType.INT)) {
      ChunkReaderContext fourByteOffsetReaderContext = fourByteOffsetReader.createContext();
      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getInt(i, fourByteOffsetReaderContext), expected[i]);
      }
    }

    FileUtils.deleteQuietly(compressedFile);
  }

  @Test(dataProvider = "combinations")
  public void testLong(ChunkCompressionType compressionType, int version)
      throws Exception {
    long[] expected = new long[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = RANDOM.nextLong();
    }

    File outFileFourByte = new File(TEST_FILE);
    File outFileEightByte = new File(TEST_FILE + "8byte");
    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    try (FixedByteChunkForwardIndexWriter fourByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileFourByte,
        compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Long.BYTES, version);
        FixedByteChunkForwardIndexWriter eightByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileEightByte,
            compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Long.BYTES, version)) {
      for (long value : expected) {
        fourByteOffsetWriter.putLong(value);
        eightByteOffsetWriter.putLong(value);
      }
    }

    try (ForwardIndexReader<ChunkReaderContext> fourByteOffsetReader = version >= 4
        ? new FixedBytePower2ChunkSVForwardIndexReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), FieldSpec.DataType.LONG)
        : new FixedByteChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), FieldSpec.DataType.LONG);
        ChunkReaderContext fourByteOffsetReaderContext = fourByteOffsetReader
            .createContext();
        ForwardIndexReader<ChunkReaderContext> eightByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), FieldSpec.DataType.LONG)
            : new FixedByteChunkSVForwardIndexReader(
                PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), FieldSpec.DataType.LONG);
        ChunkReaderContext eightByteOffsetReaderContext = eightByteOffsetReader
            .createContext()) {
      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getLong(i, fourByteOffsetReaderContext), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getLong(i, eightByteOffsetReaderContext), expected[i]);
      }

      // Validate byte range provider behaviour
      Assert.assertTrue(fourByteOffsetReader.isBufferByteRangeInfoSupported());
      Assert.assertTrue(eightByteOffsetReader.isBufferByteRangeInfoSupported());
      if (compressionType == ChunkCompressionType.PASS_THROUGH) {
        // For pass through compression, the buffer is fixed offset mapping type
        Assert.assertTrue(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(fourByteOffsetReader.getDocLength(), Long.BYTES);
        Assert.assertFalse(fourByteOffsetReader.isDocLengthInBits());

        Assert.assertTrue(eightByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(eightByteOffsetReader.getDocLength(), Long.BYTES);
        Assert.assertFalse(eightByteOffsetReader.isDocLengthInBits());
      } else {
        Assert.assertFalse(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertFalse(eightByteOffsetReader.isFixedOffsetMappingType());
      }
    }

    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);
  }

  @Test(dataProvider = "combinations")
  public void testFloat(ChunkCompressionType compressionType, int version)
      throws Exception {
    float[] expected = new float[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = RANDOM.nextFloat();
    }

    File outFileFourByte = new File(TEST_FILE);
    File outFileEightByte = new File(TEST_FILE + "8byte");
    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    try (FixedByteChunkForwardIndexWriter fourByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileFourByte,
        compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Float.BYTES, version);
        FixedByteChunkForwardIndexWriter eightByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileEightByte,
            compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Float.BYTES, version)) {
      for (float value : expected) {
        fourByteOffsetWriter.putFloat(value);
        eightByteOffsetWriter.putFloat(value);
      }
    }

    try (ForwardIndexReader<ChunkReaderContext> fourByteOffsetReader = version >= 4
        ? new FixedBytePower2ChunkSVForwardIndexReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), FieldSpec.DataType.FLOAT)
        : new FixedByteChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), FieldSpec.DataType.FLOAT);
        ChunkReaderContext fourByteOffsetReaderContext = fourByteOffsetReader
            .createContext();
        ForwardIndexReader<ChunkReaderContext> eightByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte),
            FieldSpec.DataType.FLOAT)
            : new FixedByteChunkSVForwardIndexReader(
                PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), FieldSpec.DataType.FLOAT);
        ChunkReaderContext eightByteOffsetReaderContext = eightByteOffsetReader
            .createContext()) {
      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getFloat(i, fourByteOffsetReaderContext), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getFloat(i, eightByteOffsetReaderContext), expected[i]);
      }

      // Validate byte range provider behaviour
      Assert.assertTrue(fourByteOffsetReader.isBufferByteRangeInfoSupported());
      Assert.assertTrue(eightByteOffsetReader.isBufferByteRangeInfoSupported());
      if (compressionType == ChunkCompressionType.PASS_THROUGH) {
        // For pass through compression, the buffer is fixed offset mapping type
        Assert.assertTrue(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(fourByteOffsetReader.getDocLength(), Float.BYTES);
        Assert.assertFalse(fourByteOffsetReader.isDocLengthInBits());

        Assert.assertTrue(eightByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(eightByteOffsetReader.getDocLength(), Float.BYTES);
        Assert.assertFalse(eightByteOffsetReader.isDocLengthInBits());
      } else {
        Assert.assertFalse(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertFalse(eightByteOffsetReader.isFixedOffsetMappingType());
      }
    }

    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);
  }

  @Test(dataProvider = "combinations")
  public void testDouble(ChunkCompressionType compressionType, int version)
      throws Exception {
    double[] expected = new double[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = RANDOM.nextDouble();
    }

    File outFileFourByte = new File(TEST_FILE);
    File outFileEightByte = new File(TEST_FILE + "8byte");
    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    try (FixedByteChunkForwardIndexWriter fourByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileFourByte,
        compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Double.BYTES, version);
        FixedByteChunkForwardIndexWriter eightByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileEightByte,
            compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Double.BYTES, version)) {
      for (double value : expected) {
        fourByteOffsetWriter.putDouble(value);
        eightByteOffsetWriter.putDouble(value);
      }
    }

    try (ForwardIndexReader<ChunkReaderContext> fourByteOffsetReader = version >= 4
        ? new FixedBytePower2ChunkSVForwardIndexReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), FieldSpec.DataType.DOUBLE)
        : new FixedByteChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), FieldSpec.DataType.DOUBLE);
        ChunkReaderContext fourByteOffsetReaderContext = fourByteOffsetReader
            .createContext();
        ForwardIndexReader<ChunkReaderContext> eightByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), FieldSpec.DataType.DOUBLE)
            : new FixedByteChunkSVForwardIndexReader(
                PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), FieldSpec.DataType.DOUBLE);
        ChunkReaderContext eightByteOffsetReaderContext = eightByteOffsetReader
            .createContext()) {
      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getDouble(i, fourByteOffsetReaderContext), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getDouble(i, eightByteOffsetReaderContext), expected[i]);
      }

      // Validate byte range provider behaviour
      Assert.assertTrue(fourByteOffsetReader.isBufferByteRangeInfoSupported());
      Assert.assertTrue(eightByteOffsetReader.isBufferByteRangeInfoSupported());
      if (compressionType == ChunkCompressionType.PASS_THROUGH) {
        // For pass through compression, the buffer is fixed offset mapping type
        Assert.assertTrue(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(fourByteOffsetReader.getDocLength(), Double.BYTES);
        Assert.assertFalse(fourByteOffsetReader.isDocLengthInBits());

        Assert.assertTrue(eightByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(eightByteOffsetReader.getDocLength(), Double.BYTES);
        Assert.assertFalse(eightByteOffsetReader.isDocLengthInBits());
      } else {
        Assert.assertFalse(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertFalse(eightByteOffsetReader.isFixedOffsetMappingType());
      }

      Assert.assertTrue(fourByteOffsetReader.isBufferByteRangeInfoSupported());
      Assert.assertTrue(eightByteOffsetReader.isBufferByteRangeInfoSupported());
      if (compressionType == ChunkCompressionType.PASS_THROUGH) {
        // For pass through compression, the buffer is fixed offset mapping type
        Assert.assertTrue(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(fourByteOffsetReader.getDocLength(), Double.BYTES);
        Assert.assertFalse(fourByteOffsetReader.isDocLengthInBits());

        Assert.assertTrue(eightByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(eightByteOffsetReader.getDocLength(), Double.BYTES);
        Assert.assertFalse(eightByteOffsetReader.isDocLengthInBits());
      } else {
        Assert.assertFalse(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertFalse(eightByteOffsetReader.isFixedOffsetMappingType());
      }
    }

    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);
  }



}
