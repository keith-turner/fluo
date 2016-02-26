/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.api.data;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.google.common.base.Preconditions;

/**
 * This class provides an easy, efficient, reusable mechanism for building immutable Bytes objects.
 */
public class BytesBuilder {

  private byte[] ba;
  private int len;

  BytesBuilder(int initialCapacity) {
    ba = new byte[initialCapacity];
    len = 0;
  }

  BytesBuilder() {
    this(32);
  }

  private void ensureCapacity(int min) {
    if (ba.length < min) {
      int newLen = ba.length * 2;
      if (newLen < min) {
        newLen = min;
      }

      ba = Arrays.copyOf(ba, newLen);
    }
  }

  /**
   * Converts string to bytes using UTF-8 encoding and appends bytes.
   *
   * @return self
   */
  public BytesBuilder append(String s) {
    return append(s.getBytes(StandardCharsets.UTF_8));
  }

  public BytesBuilder append(Bytes b) {
    ensureCapacity(len + b.length());
    for (int i = 0; i < b.length(); i++) {
      ba[len++] = b.byteAt(i);
    }

    return this;
  }

  public BytesBuilder append(byte[] bytes) {
    ensureCapacity(len + bytes.length);
    System.arraycopy(bytes, 0, ba, len, bytes.length);
    len += bytes.length;

    return this;
  }

  /**
   * Sets the point at which appending will start. This method can shrink or grow the ByteBuilder
   * from its current state. If it grows it will zero pad.
   */
  public void setLength(int newLen) {
    Preconditions.checkArgument(newLen >= 0, "Negative length passed : " + newLen);
    if (newLen > ba.length) {
      ba = Arrays.copyOf(ba, newLen);
    }

    if (newLen > len) {
      Arrays.fill(ba, len, newLen, (byte) 0);
    }

    len = newLen;
  }

  public int getLength() {
    return len;
  }

  public Bytes toBytes() {
    return Bytes.of(ba, 0, len);
  }
}
