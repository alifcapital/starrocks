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

package com.starrocks.sql.optimizer.statistics;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/** Exact frequencies over the shared key dictionary of a correlation tensor. */
public final class CompactDegreeVector {
    private static final int EMPTY = 0;
    private static final int BYTE = 1;
    private static final int SHORT = 2;
    private static final int INT = 3;
    private static final int LONG = 4;
    private static final int PRESENCE = 5;
    private static final int SPARSE = 6;

    private final int size;
    private final int nonZeroCount;
    private final int encoding;
    private final Object values;
    private final long[] bitmap;

    private CompactDegreeVector(int size, int nonZeroCount, int encoding, Object values, long[] bitmap) {
        this.size = size;
        this.nonZeroCount = nonZeroCount;
        this.encoding = encoding;
        this.values = values;
        this.bitmap = bitmap;
    }

    public static CompactDegreeVector copyOf(long[] frequencies) {
        int count = 0;
        long maximum = 0;
        for (long value : frequencies) {
            if (value < 0) {
                throw new IllegalArgumentException("Negative key frequency");
            }
            count += value == 0 ? 0 : 1;
            maximum = Math.max(maximum, value);
        }
        int size = frequencies.length;
        if (count == 0) {
            return new CompactDegreeVector(size, 0, EMPTY, null, null);
        }
        int wordCount = wordCount(size);
        int width = maximum <= 255 ? 1 : maximum <= 65535 ? 2 : maximum <= Integer.MAX_VALUE ? 4 : 8;
        if (maximum == 1 || 8L * (wordCount + count) < (long) width * size) {
            long[] bits = new long[wordCount];
            long[] nonZeros = maximum == 1 ? null : new long[count];
            int position = 0;
            for (int i = 0; i < size; i++) {
                if (frequencies[i] != 0) {
                    bits[i >>> 6] |= 1L << (i & 63);
                    if (nonZeros != null) {
                        nonZeros[position++] = frequencies[i];
                    }
                }
            }
            return new CompactDegreeVector(size, count, maximum == 1 ? PRESENCE : SPARSE, nonZeros, bits);
        }
        Object packed;
        int kind;
        if (width == 1) {
            byte[] array = new byte[size];
            for (int i = 0; i < size; i++) {
                array[i] = (byte) frequencies[i];
            }
            packed = array;
            kind = BYTE;
        } else if (width == 2) {
            char[] array = new char[size];
            for (int i = 0; i < size; i++) {
                array[i] = (char) frequencies[i];
            }
            packed = array;
            kind = SHORT;
        } else if (width == 4) {
            int[] array = new int[size];
            for (int i = 0; i < size; i++) {
                array[i] = (int) frequencies[i];
            }
            packed = array;
            kind = INT;
        } else {
            packed = frequencies.clone();
            kind = LONG;
        }
        return new CompactDegreeVector(size, count, kind, packed, null);
    }

    int representationHash() {
        int dataHash = switch (encoding) {
            case BYTE -> Arrays.hashCode((byte[]) values);
            case SHORT -> Arrays.hashCode((char[]) values);
            case INT -> Arrays.hashCode((int[]) values);
            case LONG, SPARSE -> Arrays.hashCode((long[]) values);
            default -> 0;
        };
        return 31 * (31 * (31 * size + encoding) + dataHash) + Arrays.hashCode(bitmap);
    }

    boolean sameRepresentation(CompactDegreeVector other) {
        if (size != other.size || encoding != other.encoding || !Arrays.equals(bitmap, other.bitmap)) {
            return false;
        }
        return switch (encoding) {
            case BYTE -> Arrays.equals((byte[]) values, (byte[]) other.values);
            case SHORT -> Arrays.equals((char[]) values, (char[]) other.values);
            case INT -> Arrays.equals((int[]) values, (int[]) other.values);
            case LONG, SPARSE -> Arrays.equals((long[]) values, (long[]) other.values);
            default -> true;
        };
    }

    public int size() {
        return size;
    }

    public int nonZeroCount() {
        return nonZeroCount;
    }

    boolean hasUnitFrequencies() {
        return encoding == EMPTY || encoding == PRESENCE;
    }

    long maximumFrequency() {
        if (encoding == EMPTY) {
            return 0;
        }
        if (encoding == PRESENCE) {
            return 1;
        }
        long maximum = 0;
        if (encoding == SPARSE) {
            for (long value : (long[]) values) {
                maximum = Math.max(maximum, value);
            }
        } else {
            for (int i = 0; i < size; i++) {
                maximum = Math.max(maximum, denseValue(i));
            }
        }
        return maximum;
    }

    /** Maximum frequency among keys present in another vector from the SAME head dictionary. */
    long maximumOnSupport(CompactDegreeVector support) {
        if (size != support.size) {
            throw new IllegalArgumentException("Different key dictionaries");
        }
        if (nonZeroCount == 0 || support.nonZeroCount == 0) {
            return 0;
        }
        long maximum = 0;
        if (isDense() && support.isDense()) {
            for (int i = 0; i < size; i++) {
                if (support.denseValue(i) != 0) {
                    maximum = Math.max(maximum, denseValue(i));
                }
            }
        } else if (isDense()) {
            for (int word = 0; word < support.bitmap.length; word++) {
                long bits = support.bitmap[word];
                while (bits != 0) {
                    maximum = Math.max(maximum, denseValue((word << 6) + Long.numberOfTrailingZeros(bits)));
                    bits &= bits - 1;
                }
            }
        } else {
            int offset = 0;
            for (int word = 0; word < bitmap.length; word++) {
                long bits = bitmap[word];
                long common = support.isDense() ? bits : bits & support.bitmap[word];
                while (common != 0) {
                    int index = (word << 6) + Long.numberOfTrailingZeros(common);
                    if (!support.isDense() || support.denseValue(index) != 0) {
                        long below = Long.lowestOneBit(common) - 1;
                        maximum = Math.max(maximum, sparseValue(offset + Long.bitCount(bits & below)));
                    }
                    common &= common - 1;
                }
                offset += Long.bitCount(bits);
            }
        }
        return maximum;
    }

    private static int wordCount(int size) {
        return (int) ((size + 63L) >>> 6);
    }

    private boolean isDense() {
        return encoding >= BYTE && encoding <= LONG;
    }

    private long denseValue(int index) {
        return switch (encoding) {
            case BYTE -> ((byte[]) values)[index] & 255;
            case SHORT -> ((char[]) values)[index];
            case INT -> ((int[]) values)[index];
            case LONG -> ((long[]) values)[index];
            default -> throw new IllegalStateException("Not a dense frequency vector");
        };
    }

    private long sparseValue(int position) {
        return encoding == PRESENCE ? 1 : ((long[]) values)[position];
    }

    /** Adds frequencies without expanding the cached representation. The target belongs to the caller. */
    public void addTo(long[] target) {
        if (target.length != size) {
            throw new IllegalArgumentException("Different key dictionaries");
        }
        if (isDense()) {
            switch (encoding) {
                case BYTE -> {
                    byte[] array = (byte[]) values;
                    for (int i = 0; i < size; i++) {
                        target[i] = Math.addExact(target[i], array[i] & 255);
                    }
                }
                case SHORT -> {
                    char[] array = (char[]) values;
                    for (int i = 0; i < size; i++) {
                        target[i] = Math.addExact(target[i], array[i]);
                    }
                }
                case INT -> {
                    int[] array = (int[]) values;
                    for (int i = 0; i < size; i++) {
                        target[i] = Math.addExact(target[i], array[i]);
                    }
                }
                default -> {
                    long[] array = (long[]) values;
                    for (int i = 0; i < size; i++) {
                        target[i] = Math.addExact(target[i], array[i]);
                    }
                }
            }
        } else if (bitmap != null) {
            int position = 0;
            for (int word = 0; word < bitmap.length; word++) {
                long bits = bitmap[word];
                while (bits != 0) {
                    int index = (word << 6) + Long.numberOfTrailingZeros(bits);
                    target[index] = Math.addExact(target[index], sparseValue(position++));
                    bits &= bits - 1;
                }
            }
        }
    }

    /** Frequency products for JOIN, frequency/presence products for either RF direction. */
    public double product(CompactDegreeVector other, boolean leftPresence, boolean rightPresence, int power) {
        if (size != other.size || power < 1 || power > 3) {
            throw new IllegalArgumentException("Incompatible frequency product");
        }
        if (nonZeroCount == 0 || other.nonZeroCount == 0) {
            return 0;
        }
        if (isDense() && other.isDense()) {
            double result = 0;
            for (int i = 0; i < size; i++) {
                result += contribution(denseValue(i), other.denseValue(i), leftPresence, rightPresence, power);
            }
            return result;
        }
        if (isDense()) {
            return other.product(this, rightPresence, leftPresence, power);
        }
        if (other.isDense()) {
            double result = 0;
            int position = 0;
            for (int word = 0; word < bitmap.length; word++) {
                long bits = bitmap[word];
                while (bits != 0) {
                    int index = (word << 6) + Long.numberOfTrailingZeros(bits);
                    result += contribution(sparseValue(position++), other.denseValue(index),
                            leftPresence, rightPresence, power);
                    bits &= bits - 1;
                }
            }
            return result;
        }
        double result = 0;
        int leftOffset = 0;
        int rightOffset = 0;
        for (int word = 0; word < bitmap.length; word++) {
            long leftWord = bitmap[word];
            long rightWord = other.bitmap[word];
            long common = leftWord & rightWord;
            while (common != 0) {
                long below = Long.lowestOneBit(common) - 1;
                long left = sparseValue(leftOffset + Long.bitCount(leftWord & below));
                long right = other.sparseValue(rightOffset + Long.bitCount(rightWord & below));
                result += contribution(left, right, leftPresence, rightPresence, power);
                common &= common - 1;
            }
            leftOffset += Long.bitCount(leftWord);
            rightOffset += Long.bitCount(rightWord);
        }
        return result;
    }

    private static double contribution(long left, long right, boolean leftPresence, boolean rightPresence, int power) {
        if (left == 0 || right == 0) {
            return 0;
        }
        double product = (leftPresence ? 1.0 : left) * (rightPresence ? 1.0 : right);
        return power == 1 ? product : power == 2 ? product * product : product * product * product;
    }

    /** Intersect sparse supports a word at a time, with running ranks rather than per-key cursor synchronization. */
    static double product(CompactDegreeVector[] vectors, int presenceMask, int power) {
        return products(vectors, presenceMask)[power - 1];
    }

    /** Three correlation moments share the same support intersection and frequency loads. */
    static double[] products(CompactDegreeVector[] vectors, int presenceMask) {
        return products(vectors, presenceMask, new double[vectors[0].size]);
    }

    static double[] products(CompactDegreeVector[] vectors, int presenceMask, double[] work) {
        double[] result = new double[3];
        int size = vectors[0].size;
        int minimum = size;
        for (CompactDegreeVector vector : vectors) {
            if (vector.size != size) {
                throw new IllegalArgumentException("Different key dictionaries");
            }
            if (vector.nonZeroCount == 0) {
                return result;
            }
            minimum = Math.min(minimum, vector.nonZeroCount);
        }
        // Dense supports benefit from typed, column-wise loops. Sparse supports use intersection below.
        if (size > 256 && minimum >= size / 8) {
            Arrays.fill(work, 0, size, 1);
            for (int side = 0; side < vectors.length; side++) {
                vectors[side].multiplyInto(work, (presenceMask & (1 << side)) != 0);
            }
            for (int i = 0; i < size; i++) {
                double value = work[i];
                result[0] += value;
                result[1] += value * value;
                result[2] += value * value * value;
            }
            return result;
        }
        int[] offsets = new int[vectors.length];
        int words = wordCount(size);
        for (int word = 0; word < words; word++) {
            long common = -1L;
            for (CompactDegreeVector vector : vectors) {
                if (vector.bitmap != null) {
                    common &= vector.bitmap[word];
                }
            }
            if (word == words - 1 && (size & 63) != 0) {
                common &= (1L << (size & 63)) - 1;
            }
            while (common != 0) {
                int index = (word << 6) + Long.numberOfTrailingZeros(common);
                long below = Long.lowestOneBit(common) - 1;
                double product = 1;
                for (int side = 0; side < vectors.length; side++) {
                    CompactDegreeVector vector = vectors[side];
                    boolean presence = (presenceMask & (1 << side)) != 0;
                    if (vector.bitmap != null && (presence || vector.encoding == PRESENCE)) {
                        continue;
                    }
                    long value = vector.isDense() ? vector.denseValue(index)
                            : vector.sparseValue(offsets[side] + Long.bitCount(vector.bitmap[word] & below));
                    if (value == 0) {
                        product = 0;
                        break;
                    }
                    if (!presence) {
                        product *= value;
                    }
                }
                result[0] += product;
                result[1] += product * product;
                result[2] += product * product * product;
                common &= common - 1;
            }
            for (int side = 0; side < vectors.length; side++) {
                if (vectors[side].bitmap != null) {
                    offsets[side] += Long.bitCount(vectors[side].bitmap[word]);
                }
            }
        }
        return result;
    }

    void multiplyInto(double[] target, boolean presence) {
        switch (encoding) {
            case EMPTY -> Arrays.fill(target, 0, size, 0);
            case BYTE -> {
                byte[] array = (byte[]) values;
                for (int i = 0; i < size; i++) {
                    target[i] *= presence ? array[i] == 0 ? 0 : 1 : array[i] & 255;
                }
            }
            case SHORT -> {
                char[] array = (char[]) values;
                for (int i = 0; i < size; i++) {
                    target[i] *= presence ? array[i] == 0 ? 0 : 1 : array[i];
                }
            }
            case INT -> {
                int[] array = (int[]) values;
                for (int i = 0; i < size; i++) {
                    target[i] *= presence ? array[i] == 0 ? 0 : 1 : array[i];
                }
            }
            case LONG -> {
                long[] array = (long[]) values;
                for (int i = 0; i < size; i++) {
                    target[i] *= presence ? array[i] == 0 ? 0 : 1 : array[i];
                }
            }
            default -> {
                int position = 0;
                for (int word = 0; word < bitmap.length; word++) {
                    int end = Math.min(size, (word + 1) << 6);
                    for (int i = word << 6; i < end; i++) {
                        if ((bitmap[word] & (1L << (i & 63))) == 0) {
                            target[i] = 0;
                        }
                    }
                    if (!presence && encoding == SPARSE) {
                        long bits = bitmap[word];
                        while (bits != 0) {
                            target[(word << 6) + Long.numberOfTrailingZeros(bits)] *= sparseValue(position++);
                            bits &= bits - 1;
                        }
                    }
                }
            }
        }
    }

    public long estimatedSize() {
        long bytes = 64;
        if (bitmap != null) {
            bytes += 24 + 8L * bitmap.length;
        }
        if (values != null) {
            bytes += 24 + switch (encoding) {
                case BYTE -> (long) size;
                case SHORT -> 2L * size;
                case INT -> 4L * size;
                case LONG -> 8L * size;
                case SPARSE -> 8L * nonZeroCount;
                default -> 0L;
            };
        }
        return (bytes + 7) & ~7L;
    }

    public void write(DataOutput out) throws IOException {
        out.writeByte(encoding);
        out.writeInt(size);
        out.writeInt(nonZeroCount);
        if (bitmap != null) {
            for (long word : bitmap) {
                out.writeLong(word);
            }
        }
        switch (encoding) {
            case BYTE -> out.write((byte[]) values);
            case SHORT -> {
                for (char value : (char[]) values) {
                    out.writeChar(value);
                }
            }
            case INT -> {
                for (int value : (int[]) values) {
                    out.writeInt(value);
                }
            }
            case LONG, SPARSE -> {
                for (long value : (long[]) values) {
                    out.writeLong(value);
                }
            }
            default -> { }
        }
    }

    /** The caller also bounds the total payload; maxSize bounds allocation for a single vector. */
    public static CompactDegreeVector read(DataInput in, int maxSize) throws IOException {
        int kind = in.readUnsignedByte();
        int size = in.readInt();
        int count = in.readInt();
        if (kind > SPARSE || size < 0 || size > maxSize || count < 0 || count > size
                || (kind == EMPTY) != (count == 0)) {
            throw new IOException("Invalid frequency vector header");
        }
        long[] bits = null;
        if (kind == PRESENCE || kind == SPARSE) {
            bits = new long[wordCount(size)];
            int actualCount = 0;
            for (int i = 0; i < bits.length; i++) {
                bits[i] = in.readLong();
                actualCount += Long.bitCount(bits[i]);
            }
            if (actualCount != count || (size % 64 != 0 && (bits[bits.length - 1] >>> (size % 64)) != 0)) {
                throw new IOException("Invalid frequency bitmap");
            }
        }
        Object packed = null;
        int actualCount = 0;
        switch (kind) {
            case BYTE -> {
                byte[] array = new byte[size];
                in.readFully(array);
                for (byte value : array) {
                    actualCount += value == 0 ? 0 : 1;
                }
                packed = array;
            }
            case SHORT -> {
                char[] array = new char[size];
                for (int i = 0; i < size; i++) {
                    array[i] = in.readChar();
                    actualCount += array[i] == 0 ? 0 : 1;
                }
                packed = array;
            }
            case INT -> {
                int[] array = new int[size];
                for (int i = 0; i < size; i++) {
                    array[i] = in.readInt();
                    if (array[i] < 0) {
                        throw new IOException("Negative key frequency");
                    }
                    actualCount += array[i] == 0 ? 0 : 1;
                }
                packed = array;
            }
            case LONG, SPARSE -> {
                long[] array = new long[kind == LONG ? size : count];
                for (int i = 0; i < array.length; i++) {
                    array[i] = in.readLong();
                    if (array[i] < 0 || (kind == SPARSE && array[i] == 0)) {
                        throw new IOException("Invalid key frequency");
                    }
                    actualCount += array[i] == 0 ? 0 : 1;
                }
                packed = array;
            }
            case PRESENCE -> actualCount = count;
            default -> { }
        }
        if (actualCount != count) {
            throw new IOException("Invalid nonzero frequency count");
        }
        return new CompactDegreeVector(size, count, kind, packed, bits);
    }
}
