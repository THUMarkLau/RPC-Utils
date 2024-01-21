package org.mine.rpc; /*
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mine.rpc.InsertRecordsSerializeInColumnUtils.totalOriginalSize;

public class DictionaryEncoderForListOfStringList {
  public static ByteBuffer encode(List<List<String>> list) {
    /*
     * Buffer structure:
     * [The number of strings] [The first string] [The second string] ...
     * [count of lists]
     * [size of first list] [The first value of first list] [The second value of first list] ... [end of first list]
     * [size of second list] [The first value of second list] [The second value of second list] ... [end of second list]
     * ...
     *
     * Each string is ended by a '\0' character.
     * Each list is ended by a -1.
     */
    int size = 0;
    Map<String, Short> stringIdMap = new LinkedHashMap<>();
    AtomicInteger cnt = new AtomicInteger(1);
    AtomicInteger bufferSize = new AtomicInteger(6); // size of dictionarySize + size of list
    for (List<String> l : list) {
      size += l.size();
      for (String string : l) {
        stringIdMap.computeIfAbsent(
            string,
            s -> {
              bufferSize.addAndGet(s.length() + 1);
              return (short) cnt.getAndIncrement();
            });
        totalOriginalSize.addAndGet(string.length());
      }
    }
    bufferSize.addAndGet(size * 2); // list id
    bufferSize.addAndGet(list.size() * 4); // list size

    ByteBuffer buffer = ByteBuffer.allocate(bufferSize.get());
    buffer.putShort((short) stringIdMap.size());
    for (String s : stringIdMap.keySet()) {
      for (int i = 0; i < s.length(); i++) {
        buffer.put((byte) s.charAt(i));
      }
      buffer.put((byte) 0);
    }
    buffer.putInt(list.size());
    for (List<String> l : list) {
      buffer.putInt(l.size());
      for (String s : l) {
        buffer.putShort((short) stringIdMap.get(s).intValue());
      }
    }
    buffer.flip();
    return buffer;
  }

  public static List<List<String>> decode(ByteBuffer buffer) {
    short dictionarySize = buffer.getShort();
    Map<Short, String> idStringMap = new LinkedHashMap<>();
    for (int i = 0; i < dictionarySize; i++) {
      StringBuilder sb = new StringBuilder();
      byte b = buffer.get();
      while (b != 0) {
        sb.append((char) b);
        b = buffer.get();
      }
      idStringMap.put((short) (i + 1), sb.toString());
    }
    int size = buffer.getInt();
    List<List<String>> output = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      int length = buffer.getInt();
      List<String> list = new ArrayList<>(length);
      for (int j = 0; j < length; ++j) {
        short id = buffer.getShort();
        list.add(idStringMap.get(id));
      }
      output.add(list);
    }
    return output;
  }

  public static void main(String[] args) {
    List<List<String>> testList = new ArrayList<>();
    List<String> list1 = Arrays.asList("s1", "s2", "s3", "s4", "s5", "s6", "s7");
    List<String> list2 = Arrays.asList("s1", "s2", "s3", "s4", "s6");
    List<String> list3 = Arrays.asList("s4", "s5", "s6", "s7");
    List<String> list4 = Arrays.asList("s1", "s2", "s3", "s4", "s5", "s6", "s7");
    List<String> list5 = Arrays.asList("s1", "s2", "s3", "s4", "s6");
    List<String> list6 = Arrays.asList("s4", "s5", "s6", "s7");
    testList.add(list1);
    testList.add(list2);
    testList.add(list3);
    testList.add(list4);
    testList.add(list5);
    testList.add(list6);
    ByteBuffer buffer = DictionaryEncoderForListOfStringList.encode(testList);
    System.out.println(buffer);
    List<List<String>> decodeList = DictionaryEncoderForListOfStringList.decode(buffer);
    System.out.println(testList.equals(decodeList));
  }
}
