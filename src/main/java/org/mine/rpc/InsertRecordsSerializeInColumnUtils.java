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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class InsertRecordsSerializeInColumnUtils {
  public static AtomicLong totalSerializeTime = new AtomicLong(0);
  public static AtomicLong totalSerializeCount = new AtomicLong(0);
  public static AtomicLong totalOriginalSize = new AtomicLong(0);
  public static AtomicLong totalCompressedSize = new AtomicLong(0);
  public static AtomicLong totalDeserializeTime = new AtomicLong(0);
  public static AtomicLong totalDeserializeCount = new AtomicLong(0);
  private static final Logger LOGGER =
      LoggerFactory.getLogger(InsertRecordsSerializeInColumnUtils.class);

  public static ByteBuffer encode(InsertRecordsReq req) throws IOException {
    long originalStartTime = System.nanoTime();
    long startTime = System.nanoTime();
    ByteBuffer deviceBuffer = DictionaryEncoderForStringList.encode(req.getPrefixPath());
    LOGGER.debug("\tTime for device: " + (System.nanoTime() - startTime) / 1000000.0 + "ms");
    startTime = System.nanoTime();
    ByteBuffer measurementBuffer =
        DictionaryEncoderForListOfStringList.encode(req.getMeasurements());
    LOGGER.debug("\tTime for measurement: " + (System.nanoTime() - startTime) / 1000000.0 + "ms");
    startTime = System.nanoTime();
    ByteBuffer valueBuffer =
        ValueEncoder.encode(req.getValues(), req.getTypes(), req.getTimestamp());
    LOGGER.debug("\tTime for value: " + (System.nanoTime() - startTime) / 1000000.0 + "ms");
    startTime = System.nanoTime();
    ByteBuffer buffer =
        ByteBuffer.allocateDirect(
            deviceBuffer.remaining()
                + measurementBuffer.remaining()
                + valueBuffer.remaining()
                + 4 * 3);
    buffer.put(deviceBuffer);
    buffer.put(measurementBuffer);
    buffer.put(valueBuffer);
    buffer.flip();
    LOGGER.debug("\tTime for concat: " + (System.nanoTime() - startTime) / 1000000.0 + "ms ");
    totalSerializeTime.addAndGet(System.nanoTime() - originalStartTime);
    long count = totalSerializeCount.incrementAndGet();
    totalCompressedSize.addAndGet(buffer.remaining());
    return buffer;
  }

  public static InsertRecordsReq decode(ByteBuffer buffer) throws IOException {
    long startTime = System.nanoTime();
    List<String> deviceList = DictionaryEncoderForStringList.decode(buffer);
    List<List<String>> measurementList = DictionaryEncoderForListOfStringList.decode(buffer);
    List<List<Object>> values = new ArrayList<>();
    List<List<TSDataType>> dataTypes = new ArrayList<>();
    List<Long> timestamp = new ArrayList<>();
    ValueEncoder.decode(buffer, values, dataTypes, timestamp);
    InsertRecordsReq req =
        new InsertRecordsReq(deviceList, measurementList, dataTypes, values, timestamp);
    totalDeserializeTime.addAndGet(System.nanoTime() - startTime);
    long count = totalDeserializeCount.incrementAndGet();
    if (count % 1000 == 0) {
      LOGGER.error(
          "Average deserialize time of new column format: "
              + totalDeserializeTime.get() * 1.0 / count / 1000000.0
              + "ms");
      totalDeserializeCount.set(0);
      totalDeserializeTime.set(0);
    }
    return req;
  }

  public static void main(String[] args) {
    for (int i = 0; i < Integer.MAX_VALUE - 1; ++i) {
      try {
        test();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void test() throws IOException {
    List<List<TSDataType>> types = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    List<String> device = new ArrayList<>();
    List<List<String>> measurements = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    Random random = new Random();
    long originSize = 0;
    for (int i = 0; i < 200; ++i) {
      for (int k = 0; k < 100; ++k) {
        List<TSDataType> typeList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        List<String> measurement = new ArrayList<>();
        for (int j = 0, size = random.nextInt(20) + 1; j < size; ++j) {
          measurement.add("s" + j);
          int type = random.nextInt(5);
          switch (type) {
            case 0:
              typeList.add(TSDataType.INT32);
              valueList.add(10);
              originSize += 4;
              break;
            case 1:
              typeList.add(TSDataType.INT64);
              valueList.add(20L);
              originSize += 8;
              break;
            case 2:
              typeList.add(TSDataType.FLOAT);
              valueList.add(1.1f);
              originSize += 4;
              break;
            case 3:
              typeList.add(TSDataType.DOUBLE);
              valueList.add(2.0d);
              originSize += 8;
              break;
            case 4:
              typeList.add(TSDataType.BOOLEAN);
              valueList.add(true);
              originSize += 1;
              break;
            case 5:
              typeList.add(TSDataType.TEXT);
              valueList.add("This is a test string.");
              originSize += 22;
              break;
          }
        }
        types.add(typeList);
        originSize += typeList.size();
        values.add(valueList);
        timestamps.add((long) k);
        originSize += 8;
        measurements.add(measurement);
        originSize += measurement.size() * 2L;
        device.add("root.test.sg_0.d" + i);
        originSize += device.get(i).length();
      }
    }
    InsertRecordsReq req = new InsertRecordsReq(device, measurements, types, values, timestamps);
    long startTime = System.nanoTime();
    ByteBuffer buffer = encode(req);
    long endTime = System.nanoTime();
    LOGGER.debug("Encoding Time: " + (endTime - startTime) / 1000000.0 + "ms");
    startTime = System.nanoTime();
    InsertRecordsReq decodedReq = decode(buffer);
    if (!req.equals(decodedReq)) {
      LOGGER.debug("Error!");
      System.exit(-1);
    }
    endTime = System.nanoTime();
    LOGGER.debug("Decoding Time: " + (endTime - startTime) / 1000000.0 + "ms");
    LOGGER.debug("Original size: " + originSize);
    LOGGER.debug("Compressed size: " + buffer.remaining());
    LOGGER.debug("Compression ratio: " + (double) originSize / buffer.remaining());
    LOGGER.debug("=====================================================================");
  }
}
