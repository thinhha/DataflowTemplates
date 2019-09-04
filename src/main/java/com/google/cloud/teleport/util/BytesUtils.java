/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.util;

import com.google.protobuf.ByteOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The {@link BytesUtils} class provides common utilities for handling bytes, such as
 * extracting a byte array from a ByteString without copy.
 */
public class BytesUtils {

    /**
     * Extracts the byte array from the given {@link ByteString} without copy.
     *
     * @param byteString A {@link ByteString} from which to extract the array.
     * @return an array of byte.
     */
    public static byte[] toByteArray(final ByteString byteString) {
        try {
            ZeroCopyByteOutput byteOutput = new ZeroCopyByteOutput();
            UnsafeByteOperations.unsafeWriteTo(byteString, byteOutput);
            return byteOutput.bytes;
        } catch (IOException e) {
            return byteString.toByteArray();
        }
    }

    private static final class ZeroCopyByteOutput extends ByteOutput {
        private byte[] bytes;

        @Override
        public void writeLazy(byte[] value, int offset, int length) {
            if (offset != 0 || length != value.length) {
                throw new UnsupportedOperationException();
            }
            bytes = value;
        }

        @Override
        public void write(byte value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(byte[] value, int offset, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(ByteBuffer value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeLazy(ByteBuffer value) {
            throw new UnsupportedOperationException();
        }
    }
}
