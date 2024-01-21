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

import java.util.List;

public class InsertRecordsReq {
  private List<String> prefixPath;
  private List<List<String>> measurements;
  private List<List<TSDataType>> types;
  private List<List<Object>> values;
  private List<Long> timestamp;

  public InsertRecordsReq() {}

  public InsertRecordsReq(
      List<String> prefixPath,
      List<List<String>> measurements,
      List<List<TSDataType>> types,
      List<List<Object>> values,
      List<Long> timestamp) {
    this.prefixPath = prefixPath;
    this.measurements = measurements;
    this.types = types;
    this.values = values;
    this.timestamp = timestamp;
  }

  public List<String> getPrefixPath() {
    return prefixPath;
  }

  public void setPrefixPath(List<String> prefixPath) {
    this.prefixPath = prefixPath;
  }

  public List<List<String>> getMeasurements() {
    return measurements;
  }

  public void setMeasurements(List<List<String>> measurements) {
    this.measurements = measurements;
  }

  public List<List<TSDataType>> getTypes() {
    return types;
  }

  public void setTypes(List<List<TSDataType>> types) {
    this.types = types;
  }

  public List<List<Object>> getValues() {
    return values;
  }

  public void setValues(List<List<Object>> values) {
    this.values = values;
  }

  public List<Long> getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(List<Long> timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof InsertRecordsReq)) return false;

    InsertRecordsReq that = (InsertRecordsReq) o;

    if (getPrefixPath() != null
        ? !getPrefixPath().equals(that.getPrefixPath())
        : that.getPrefixPath() != null) return false;
    if (getMeasurements() != null
        ? !getMeasurements().equals(that.getMeasurements())
        : that.getMeasurements() != null) return false;
    if (getTypes() != null ? !getTypes().equals(that.getTypes()) : that.getTypes() != null)
      return false;
    if (getValues() != null ? !getValues().equals(that.getValues()) : that.getValues() != null)
      return false;
    return getTimestamp() != null
        ? getTimestamp().equals(that.getTimestamp())
        : that.getTimestamp() == null;
  }
}
