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

package com.starrocks.sql.parser;

import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableSampleClause;
import com.starrocks.type.ArrayType;
import com.starrocks.type.Type;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Digest of every instance field of a parsed AST, including positions, so two parsers can be compared without
 * equals() of the AST classes. We expect the fast parser and AstBuilder to build the same object graph, so a
 * different digest is a parser bug. difference() names the first field where two ASTs differ.
 */
final class AstDigest {
    private static final Map<Class<?>, List<Field>> FIELDS = new HashMap<>();

    private final boolean randomSampleSeed;
    private final IdentityHashMap<Object, byte[]> memo = new IdentityHashMap<>();
    private final IdentityHashMap<Object, Boolean> active = new IdentityHashMap<>();

    private AstDigest(boolean randomSampleSeed) {
        this.randomSampleSeed = randomSampleSeed;
    }

    /**
     * Returns null when the ASTs are the same, else the path of the first different field. With
     * randomSampleSeed the seed of a SAMPLE clause is not compared: the SQL has no 'seed' property.
     */
    static String difference(Object a, Object b, boolean randomSampleSeed) {
        return new AstDigest(randomSampleSeed).difference(a, new AstDigest(randomSampleSeed), b, "statement");
    }

    private String difference(Object a, AstDigest other, Object b, String path) {
        if (Arrays.equals(node(a), other.node(b))) {
            return null;
        }
        if (a instanceof Collection<?> && b instanceof Collection<?>) {
            return collectionDifference((Collection<?>) a, other, (Collection<?>) b, path);
        }
        if (a == null || b == null || a.getClass() != b.getClass() || isLeaf(a)) {
            return path + ": " + describe(a) + " vs " + describe(b);
        }
        if (a instanceof Optional<?> left) {
            Optional<?> right = (Optional<?>) b;
            if (left.isPresent() != right.isPresent()) {
                return path + ": " + describe(a) + " vs " + describe(b);
            }
            return difference(left.get(), other, right.get(), path + ".get()");
        }
        if (a instanceof Map<?, ?> || a.getClass().isArray()) {
            return path + ": " + describe(a) + " vs " + describe(b);
        }
        try {
            for (Field field : fields(a.getClass())) {
                if (a instanceof CTERelation && field.getName().equals("cteMouldId")
                        || randomSampleSeed && a instanceof TableSampleClause && field.getName().equals("randomSeed")) {
                    continue;
                }
                String found = difference(field.get(a), other, field.get(b), path + "." + field.getName());
                if (found != null) {
                    return found;
                }
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        return path + ": digest differs only in ordering";
    }

    private String collectionDifference(Collection<?> left, AstDigest other, Collection<?> right, String path) {
        if (left.size() != right.size()) {
            return path + ": size " + left.size() + " vs " + right.size();
        }
        Iterator<?> l = left.iterator();
        Iterator<?> r = right.iterator();
        for (int i = 0; l.hasNext(); i++) {
            String found = difference(l.next(), other, r.next(), path + "[" + i + "]");
            if (found != null) {
                return found;
            }
        }
        return path + ": collection order";
    }

    private static boolean isLeaf(Object value) {
        return value instanceof String || value instanceof Number || value instanceof Boolean
                || value instanceof Character || value instanceof Enum<?> || value instanceof Type
                || value instanceof LocalDateTime || value instanceof UUID || value instanceof Pattern;
    }

    private static String describe(Object value) {
        if (value == null) {
            return "null";
        }
        String text = value instanceof Type type ? safeTypeSql(type) : String.valueOf(value);
        if (text.length() > 120) {
            text = text.substring(0, 120) + "...";
        }
        return value.getClass().getSimpleName() + "(" + text + ")";
    }

    private static String safeTypeSql(Type type) {
        try {
            return type.toSql();
        } catch (NullPointerException e) {
            return "malformed type";
        }
    }

    private static final MessageDigest PROTOTYPE = newSha();

    private static MessageDigest newSha() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    // A provider lookup per AST node dominates the time of a large AST, so we clone one instance.
    private static MessageDigest sha() {
        try {
            return (MessageDigest) PROTOTYPE.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    private static void frame(MessageDigest out, byte[] value) {
        int n = value.length;
        out.update((byte) (n >>> 24));
        out.update((byte) (n >>> 16));
        out.update((byte) (n >>> 8));
        out.update((byte) n);
        out.update(value);
    }

    private static void text(MessageDigest out, String value) {
        frame(out, value.getBytes(StandardCharsets.UTF_8));
    }

    private static synchronized List<Field> fields(Class<?> cls) {
        return FIELDS.computeIfAbsent(cls, key -> {
            List<Field> list = new ArrayList<>();
            for (Class<?> c = key; c != Object.class; c = c.getSuperclass()) {
                for (Field f : c.getDeclaredFields()) {
                    if (!Modifier.isStatic(f.getModifiers()) && !f.isSynthetic()) {
                        f.setAccessible(true);
                        list.add(f);
                    }
                }
            }
            list.sort(Comparator.comparing(f -> f.getDeclaringClass().getName() + "." + f.getName()));
            return list;
        });
    }

    // Hints with equal compareTo() may come in either order, so we sort them also by position.
    private byte[] hints(Object value) {
        if (value == null) {
            return node(null);
        }
        List<?> input = (List<?>) value;
        MessageDigest out = sha();
        text(out, "query-scope-hints");
        text(out, Integer.toString(input.size()));
        for (Object hint : input) {
            text(out, ((HintNode) hint).toSql());
        }
        List<HintNode> ordered = new ArrayList<>();
        for (Object hint : input) {
            ordered.add((HintNode) hint);
        }
        ordered.sort(Comparator.<HintNode>naturalOrder()
                .thenComparingInt(h -> h.getPos().getLine())
                .thenComparingInt(h -> h.getPos().getCol())
                .thenComparingInt(h -> h.getPos().getEndLine())
                .thenComparingInt(h -> h.getPos().getEndCol()));
        for (HintNode hint : ordered) {
            frame(out, node(hint));
        }
        return out.digest();
    }

    private byte[] node(Object value) {
        if (value == null) {
            MessageDigest out = sha();
            text(out, "null");
            return out.digest();
        }
        byte[] known = memo.get(value);
        if (known != null) {
            return known;
        }
        byte[] result = digest(value);
        memo.put(value, result);
        return result;
    }

    private byte[] digest(Object value) {
        MessageDigest out = sha();
        if (value instanceof String || value instanceof Number || value instanceof Boolean
                || value instanceof Character || value instanceof Enum<?>) {
            text(out, "scalar");
            text(out, value.getClass().getName());
            text(out, value instanceof Enum<?> e ? e.name() : value.toString());
            return out.digest();
        }
        if (value instanceof LocalDateTime || value instanceof UUID) {
            text(out, "scalar");
            text(out, value.getClass().getName());
            text(out, value.toString());
            return out.digest();
        }
        if (value instanceof Pattern pattern) {
            text(out, Pattern.class.getName());
            text(out, pattern.pattern());
            text(out, Integer.toString(pattern.flags()));
            return out.digest();
        }
        if (value instanceof Type type) {
            text(out, "type");
            text(out, type.getClass().getName());
            try {
                text(out, type.toSql());
                return out.digest();
            } catch (NullPointerException malformed) {
                // A type with null children cannot print itself. Its fields are digested below.
                text(out, type instanceof ArrayType ? "malformed-array-type" : "malformed-type");
            }
        }
        if (active.put(value, Boolean.TRUE) != null) {
            throw new IllegalStateException("Unexpected cycle in parsed AST: " + value.getClass());
        }
        try {
            if (value instanceof Optional<?> optional) {
                text(out, "optional");
                text(out, optional.isPresent() ? "present" : "absent");
                if (optional.isPresent()) {
                    frame(out, node(optional.get()));
                }
            } else if (value instanceof Collection<?> collection) {
                text(out, "collection");
                text(out, Integer.toString(collection.size()));
                for (Object element : collection) {
                    frame(out, node(element));
                }
            } else if (value instanceof Map<?, ?> map) {
                text(out, "map");
                text(out, Integer.toString(map.size()));
                List<byte[]> entries = new ArrayList<>();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    MessageDigest item = sha();
                    frame(item, node(entry.getKey()));
                    frame(item, node(entry.getValue()));
                    entries.add(item.digest());
                }
                entries.sort(Arrays::compareUnsigned);
                for (byte[] entry : entries) {
                    frame(out, entry);
                }
            } else if (value.getClass().isArray()) {
                text(out, "array");
                text(out, value.getClass().getName());
                int length = Array.getLength(value);
                text(out, Integer.toString(length));
                for (int i = 0; i < length; i++) {
                    frame(out, node(Array.get(value, i)));
                }
            } else {
                objectFields(out, value);
            }
            return out.digest();
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        } finally {
            active.remove(value);
        }
    }

    private void objectFields(MessageDigest out, Object value) throws IllegalAccessException {
        if (!value.getClass().getName().startsWith("com.starrocks.")) {
            throw new IllegalStateException("AST contains a type the digest does not know: " + value.getClass());
        }
        text(out, "object");
        text(out, value.getClass().getName());
        if (value instanceof CTERelation cte
                && cte.getCteMouldId() != System.identityHashCode(cte.getCteQueryStatement().getQueryRelation())) {
            throw new IllegalStateException("CTE id is not the identity hash of its query relation");
        }
        for (Field f : fields(value.getClass())) {
            text(out, f.getDeclaringClass().getName() + "." + f.getName());
            if (randomSampleSeed && value instanceof TableSampleClause && f.getName().equals("randomSeed")) {
                // Without a 'seed' property every parse draws a new random seed.
                text(out, "random-seed");
            } else if (value instanceof CTERelation && f.getName().equals("cteMouldId")) {
                // Identity hash: it differs between any two parses. We checked how it is derived above.
                text(out, "identity-derived");
            } else if (f.getDeclaringClass() == StatementBase.class && f.getName().equals("allQueryScopeHints")) {
                frame(out, hints(f.get(value)));
            } else {
                frame(out, node(f.get(value)));
            }
        }
    }
}
