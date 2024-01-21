/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.mine.rpc;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mine.rpc.InsertRecordsSerializeInColumnUtils.totalOriginalSize;

public class ValueEncoder {
  public static final Logger LOGGER = LoggerFactory.getLogger(ValueEncoder.class);
  private static final int ARRAY_SIZE = 4096;
  private static LZ4Factory factory = LZ4Factory.fastestInstance();
  private static LZ4Compressor compressor = factory.fastCompressor();
  private static LZ4FastDecompressor decompressor = factory.fastDecompressor();
  private static LZ4SafeDecompressor safeDecompressor = factory.safeDecompressor();

  public static ByteBuffer encode(
      List<List<Object>> values, List<List<TSDataType>> types, List<Long> timestamps)
      throws IOException {
    /*
     * The structure of the encoded data is as follows:
     * [Number of lists]
     * [Datatype of first value in the first list] [Datatype of second value in the first list] ... [end of first list]
     * [Datatype of first value in the second list] [Datatype of second value in the second list] ... [end of second list]
     * ...
     * [First value of first list] [Second value of first list] ... [The last value of first list]
     * [First value of second list] [Second value of second list] ... [The last value of second list]
     * ...
     *
     * The number of lists is encoded as an integer.
     * The end of each list is encoded as -1.
     * The string value is encoded as a string, and the end of the string is encoded as '\0'.
     */

    // calculate the size of buffer
    long originStartTime = System.nanoTime();
    long startTime = System.nanoTime();
    AtomicInteger bufferSize = new AtomicInteger(0);
    bufferSize.addAndGet(4); // size of lists
    SerializationBuffer valueBuffer = new SerializationBuffer(4096);
    LOGGER.debug(
        "\t\tTime for calculating buffer size = "
            + (System.nanoTime() - startTime) / 1000000.0
            + "ms");
    startTime = System.nanoTime();
    // start to encode
    valueBuffer.writeInt(values.size()); // number of lists
    // encode type lists
    for (List<TSDataType> typeList : types) {
      for (TSDataType type : typeList) {
        valueBuffer.write(type.getType());
      }
      totalOriginalSize.addAndGet(typeList.size());
      valueBuffer.write((byte) -1);
    }
    LOGGER.debug(
        "\t\tTime for encoding type lists = " + (System.nanoTime() - startTime) / 1000000.0 + "ms");
    startTime = System.nanoTime();
    // encode value list
    for (int i = 0; i < values.size(); ++i) {
      List<TSDataType> typeList = types.get(i);
      List<Object> valueList = values.get(i);
      for (int j = 0; j < typeList.size(); ++j) {
        TSDataType type = typeList.get(j);
        switch (type) {
          case INT32:
            valueBuffer.writeInt((Integer) valueList.get(j));
            totalOriginalSize.addAndGet(4);
            break;
          case INT64:
            valueBuffer.writeLong((Long) valueList.get(j));
            totalOriginalSize.addAndGet(8);
            break;
          case FLOAT:
            valueBuffer.writeFloat((Float) valueList.get(j));
            totalOriginalSize.addAndGet(4);
            break;
          case DOUBLE:
            valueBuffer.writeDouble((Double) valueList.get(j));
            totalOriginalSize.addAndGet(8);
            break;
          case BOOLEAN:
            valueBuffer.writeBoolean((Boolean) valueList.get(j));
            totalOriginalSize.addAndGet(1);
            break;
          case TEXT:
            String value = (String) valueList.get(j);
            byte[] bytes = value.getBytes();
            valueBuffer.writeInt(bytes.length);
            valueBuffer.write(bytes);
            totalOriginalSize.addAndGet(bytes.length);
            break;
        }
      }
    }
    LOGGER.debug(
        "\t\tTime for encoding value lists = "
            + (System.nanoTime() - startTime) / 1000000.0
            + "ms");
    startTime = System.nanoTime();
    ICompressor compressor = new ICompressor.IOTDBLZ4Compressor();
    byte[] compressed = compressor.compress(valueBuffer.getBuf());
    long time = System.nanoTime() - startTime;
    LOGGER.debug("\t\tTime for compressing values = " + time / 1000000.0 + "ms");

    startTime = System.nanoTime();
    Encoder encoder =
        TSEncodingBuilder.getEncodingBuilder(TSEncoding.TS_2DIFF).getEncoder(TSDataType.INT64);
    PublicBAOS timeBuffer = new PublicBAOS();
    for (long timestamp : timestamps) {
      encoder.encode(timestamp, timeBuffer);
    }
    totalOriginalSize.addAndGet(timestamps.size() * 8L);
    encoder.flush(timeBuffer);
    byte[] timeBytes = timeBuffer.toByteArray();
    LOGGER.debug(
        "\t\tTime for encoding timestamps = " + (System.nanoTime() - startTime) / 1000000.0 + "ms");
    startTime = System.nanoTime();
    // try to decode the times
    ByteBuffer buffer = ByteBuffer.allocateDirect(compressed.length + timeBytes.length + 8);
    buffer.putInt(compressed.length);
    buffer.put(compressed);
    buffer.putInt(timeBytes.length);
    buffer.put(timeBytes);
    buffer.flip();
    LOGGER.debug("\t\tTime for concat = " + (System.nanoTime() - startTime) / 1000000.0 + "ms");
    LOGGER.debug(
        "total time for value encoding is "
            + (System.nanoTime() - originStartTime) / 1000000.0
            + "ms");
    return buffer;
  }

  public static void decode(
      ByteBuffer buffer,
      List<List<Object>> values,
      List<List<TSDataType>> dataTypes,
      List<Long> timestamps)
      throws IOException {
    int dataSize = buffer.getInt();
    byte[] dataByteArray = new byte[dataSize];
    buffer.get(dataByteArray);
    IUnCompressor unCompressor = new IUnCompressor.LZ4UnCompressor();
    byte[] uncompressed = unCompressor.uncompress(dataByteArray);
    ByteBuffer uncompressedDataBuffer = ByteBuffer.wrap(uncompressed);
    int listSize = uncompressedDataBuffer.getInt();
    for (int i = 0; i < listSize; ++i) {
      List<TSDataType> typeList = new ArrayList<>();
      byte type = uncompressedDataBuffer.get();
      while (type != -1) {
        typeList.add(TSDataType.deserialize(type));
        type = uncompressedDataBuffer.get();
      }
      dataTypes.add(typeList);
    }
    for (int i = 0; i < listSize; ++i) {
      List<TSDataType> typeList = dataTypes.get(i);
      List<Object> valueList = new ArrayList<>(typeList.size());
      for (TSDataType type : typeList) {
        switch (type) {
          case INT32:
            valueList.add(uncompressedDataBuffer.getInt());
            break;
          case INT64:
            valueList.add(uncompressedDataBuffer.getLong());
            break;
          case FLOAT:
            valueList.add(uncompressedDataBuffer.getFloat());
            break;
          case DOUBLE:
            valueList.add(uncompressedDataBuffer.getDouble());
            break;
          case BOOLEAN:
            valueList.add(uncompressedDataBuffer.get() == (byte) 1);
            break;
          case TEXT:
            int size = uncompressedDataBuffer.getInt();
            byte[] bytes = new byte[size];
            uncompressedDataBuffer.get(bytes);
            valueList.add(new Binary(bytes));
            break;
        }
      }
      values.add(valueList);
    }

    int timeSize = buffer.getInt();
    byte[] timeByteArray = new byte[timeSize];
    buffer.get(timeByteArray, 0, timeSize);
    Decoder decoder = Decoder.getDecoderByType(TSEncoding.TS_2DIFF, TSDataType.INT64);
    ByteBuffer timeBuffer = ByteBuffer.wrap(timeByteArray);
    while (decoder.hasNext(timeBuffer)) {
      timestamps.add(decoder.readLong(timeBuffer));
    }
  }

  public static void main(String[] args) throws IOException {
    // write a test case
    List<List<Object>> values = new ArrayList<>();
    List<List<TSDataType>> types = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    for (int i = 0; i < 100; ++i) {
      List<Object> valueList = new ArrayList<>();
      List<TSDataType> typeList = new ArrayList<>();
      for (int j = 0; j < 100; ++j) {
        valueList.add(String.valueOf(i));
        typeList.add(TSDataType.TEXT);
      }
      values.add(valueList);
      types.add(typeList);
      timestamps.add(System.currentTimeMillis());
    }
    ByteBuffer buffer = encode(values, types, timestamps);
    List<List<Object>> decodedValues = new ArrayList<>();
    List<List<TSDataType>> decodedTypes = new ArrayList<>();
    List<Long> decodedTimestamps = new ArrayList<>();
    decode(buffer, decodedValues, decodedTypes, decodedTimestamps);
    System.out.println(decodedValues.equals(values));
    System.out.println(decodedTypes.equals(types));
    System.out.println(decodedTimestamps.equals(timestamps));
  }
}
