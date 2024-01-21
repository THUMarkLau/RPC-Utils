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

import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.IOException;

public class SerializationBuffer extends PublicBAOS {
  public SerializationBuffer() {
    super();
  }

  public SerializationBuffer(int size) {
    super(size);
  }

  public void writeLong(long value) throws IOException {
    byte[] bytes = new byte[8];
    bytes[0] = (byte) (value >>> 56);
    bytes[1] = (byte) (value >>> 48);
    bytes[2] = (byte) (value >>> 40);
    bytes[3] = (byte) (value >>> 32);
    bytes[4] = (byte) (value >>> 24);
    bytes[5] = (byte) (value >>> 16);
    bytes[6] = (byte) (value >>> 8);
    bytes[7] = (byte) value;
    write(bytes);
  }

  public void writeInt(int value) throws IOException {
    byte[] bytes = new byte[4];
    bytes[0] = (byte) (value >>> 24);
    bytes[1] = (byte) (value >>> 16);
    bytes[2] = (byte) (value >>> 8);
    bytes[3] = (byte) value;
    write(bytes);
  }

  public void writeFloat(float value) throws IOException {
    writeInt(Float.floatToIntBits(value));
  }

  public void writeDouble(double value) throws IOException {
    writeLong(Double.doubleToLongBits(value));
  }

  public void writeBoolean(boolean value) throws IOException {
    write(value ? 1 : 0);
  }
}
