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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mine.rpc.InsertRecordsSerializeInColumnUtils.totalOriginalSize;

public class DictionaryEncoderForStringList {
  public static ByteBuffer encode(List<String> inputs) throws IOException {
    // only consider the ascii string
    Map<String, Short> stringIdMap = new LinkedHashMap<>();
    AtomicInteger cnt = new AtomicInteger(1);
    for (String s : inputs) {
      stringIdMap.computeIfAbsent(s, k -> (short) cnt.getAndIncrement());
      totalOriginalSize.addAndGet(s.length());
    }

    int bufferSize = 2;
    for (String s : stringIdMap.keySet()) {
      bufferSize += s.length() + 1;
    }
    bufferSize += 4;
    bufferSize += inputs.size() * 2;
    ByteBuffer output = ByteBuffer.allocate(bufferSize);
    /*
     * Buffer structure:
     * [The number of strings] [The first string] [The second string] ...
     * [The id of first value] [The id of second value] ...
     *
     * Each string is ended by a '\0' character.
     */
    output.putShort((short) stringIdMap.size());
    for (String s : stringIdMap.keySet()) {
      for (int i = 0; i < s.length(); i++) {
        output.put((byte) s.charAt(i));
      }
      output.put((byte) 0);
    }
    output.putInt(inputs.size());
    for (String s : inputs) {
      output.putShort((short) stringIdMap.get(s).intValue());
    }
    output.flip();
    if (RPCUtilsConfig.useSchemaCompression) {
      ICompressor compressor = new ICompressor.IOTDBLZ4Compressor();
      byte[] compressed = compressor.compress(output.array());
      ByteBuffer tempBuffer = ByteBuffer.allocateDirect(compressed.length + 5);
      tempBuffer.put(RPCUtilsConstant.COMPRESSED);
      tempBuffer.putInt(compressed.length);
      tempBuffer.put(compressed);
      output = tempBuffer;
    } else {
      ByteBuffer tempBuffer = ByteBuffer.allocateDirect(output.remaining() + 1);
      tempBuffer.put(RPCUtilsConstant.RAW);
      tempBuffer.put(output);
      output = tempBuffer;
    }
    output.flip();
    return output;
  }

  public static List<String> decode(ByteBuffer input) {
    byte flag = input.get();
    if (flag == RPCUtilsConstant.COMPRESSED) {
      int length = input.getInt();
      byte[] compressed = new byte[length];
      input.get(compressed);
      try {
        IUnCompressor unCompressor = new IUnCompressor.LZ4UnCompressor();
        input = ByteBuffer.wrap(unCompressor.uncompress(compressed));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    short dictionarySize = input.getShort();
    Map<Short, String> idStringMap = new HashMap<>();
    for (int i = 0; i < dictionarySize; i++) {
      StringBuilder sb = new StringBuilder();
      byte b = input.get();
      while (b != 0) {
        sb.append((char) b);
        b = input.get();
      }
      idStringMap.put((short) (i + 1), sb.toString());
    }
    int size = input.getInt();
    List<String> output = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      short id = input.getShort();
      output.add(idStringMap.get(id));
    }
    return output;
  }
}
