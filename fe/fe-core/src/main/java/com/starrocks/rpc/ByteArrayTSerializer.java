// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.rpc;

import org.apache.thrift.TBase;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.Arrays;

/**
 * A TSerializer that writes into a plain growable byte array. The stock serializer writes through a stream transport
 * into a ByteArrayOutputStream, whose write is synchronized, and the protocols write a plan one byte or one varint at
 * a time. The protocol is the same, so the bytes are the same.
 *
 * <p>Like TSerializer, an instance is not thread safe.
 */
public class ByteArrayTSerializer extends TSerializer {
    private final ByteArrayTransport transport = new ByteArrayTransport();
    private final TProtocol protocol;

    public ByteArrayTSerializer(TProtocolFactory protocolFactory) throws TTransportException {
        super(protocolFactory);
        this.protocol = protocolFactory.getProtocol(transport);
    }

    @Override
    public byte[] serialize(TBase<?, ?> base) throws TException {
        transport.reset();
        base.write(protocol);
        return transport.toByteArray();
    }

    private static final class ByteArrayTransport extends TTransport {
        // The protocol factories set limits on the configuration of the transport, so it has one of its own.
        private final TConfiguration configuration = new TConfiguration();
        private byte[] buffer = new byte[4096];
        private int size;

        void reset() {
            size = 0;
        }

        byte[] toByteArray() {
            return Arrays.copyOf(buffer, size);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            int end = size + length;
            if (end > buffer.length) {
                buffer = Arrays.copyOf(buffer, Math.max(end, buffer.length * 2));
            }
            if (length == 1) {
                buffer[size] = bytes[offset];
            } else {
                System.arraycopy(bytes, offset, buffer, size, length);
            }
            size = end;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void open() {
        }

        @Override
        public void close() {
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws TTransportException {
            throw new TTransportException(TTransportException.NOT_OPEN, "a serializer transport cannot be read");
        }

        @Override
        public TConfiguration getConfiguration() {
            return configuration;
        }

        @Override
        public void updateKnownMessageSize(long size) {
        }

        @Override
        public void checkReadBytesAvailable(long numBytes) {
        }
    }
}
