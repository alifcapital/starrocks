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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.type.AnyArrayType;
import com.starrocks.type.AnyElementType;
import com.starrocks.type.AnyMapType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.CharType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.MapType;
import com.starrocks.type.NullType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FunctionSetTest {

    private FunctionSet functionSet;

    private static final Type VARCHAR_ARRAY = new ArrayType(VarcharType.VARCHAR);
    private static final Type TINYINT_ARRAY = new ArrayType(IntegerType.TINYINT);
    private static final Type INT_ARRAY = new ArrayType(IntegerType.INT);
    private static final Type DOUBLE_ARRAY = new ArrayType(FloatType.DOUBLE);
    private static final Type INT_ARRAY_ARRAY = new ArrayType(INT_ARRAY);
    private static final Type TINYINT_ARRAY_ARRAY = new ArrayType(TINYINT_ARRAY);
    private static final Type VARCHAR_ARRAY_ARRAY = new ArrayType(VARCHAR_ARRAY);

    @BeforeEach
    public void setUp() {
        functionSet = new FunctionSet();
        functionSet.init();
    }

    @Test
    public void testGetLagFunction() {
        Type[] argTypes1 = {DecimalType.DECIMALV2, IntegerType.TINYINT, IntegerType.TINYINT};
        Function lagDesc1 = new Function(new FunctionName(FunctionSet.LAG), argTypes1, InvalidType.INVALID, false);
        Function newFunction = functionSet.getFunction(lagDesc1, Function.CompareMode.IS_SUPERTYPE_OF);
        Type[] newArgTypes = newFunction.getArgs();
        Assertions.assertTrue(newArgTypes[0].matchesType(newArgTypes[2]));
        Assertions.assertTrue(newArgTypes[0].matchesType(DecimalType.DECIMALV2));

        Type[] argTypes2 = {VarcharType.VARCHAR, IntegerType.TINYINT, IntegerType.TINYINT};
        Function lagDesc2 = new Function(new FunctionName(FunctionSet.LAG), argTypes2, InvalidType.INVALID, false);
        newFunction = functionSet.getFunction(lagDesc2, Function.CompareMode.IS_SUPERTYPE_OF);
        newArgTypes = newFunction.getArgs();
        Assertions.assertTrue(newArgTypes[0].matchesType(newArgTypes[2]));
        Assertions.assertTrue(newArgTypes[0].matchesType(VarcharType.VARCHAR));
    }

    @Test
    public void testPolymorphicFunction() {
        // array_append(ARRAY<INT>, INT)
        Type[] argTypes = {INT_ARRAY, IntegerType.INT};
        Function desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        Function fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(IntegerType.INT, fn.getArgs()[1]);

        // array_append(ARRAY<INT>, TINYINT)
        argTypes = new Type[] {INT_ARRAY, IntegerType.TINYINT};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(IntegerType.INT, fn.getArgs()[1]);

        // array_append(ARRAY<TINYINT>, INT)
        argTypes = new Type[] {TINYINT_ARRAY, IntegerType.INT};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(IntegerType.INT, fn.getArgs()[1]);

        // array_append(ARRAY<INT>, DOUBLE)
        argTypes = new Type[] {INT_ARRAY, FloatType.DOUBLE};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(DOUBLE_ARRAY, fn.getReturnType());
        Assertions.assertEquals(DOUBLE_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(FloatType.DOUBLE, fn.getArgs()[1]);

        // array_append(NULL, INT)
        argTypes = new Type[] {NullType.NULL, IntegerType.INT};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(IntegerType.INT, fn.getArgs()[1]);

        // array_append(ARRAY<INT>, NULL)
        argTypes = new Type[] {INT_ARRAY, NullType.NULL};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(IntegerType.INT, fn.getArgs()[1]);

        // array_append(ARRAY<TINYINT>, VARCHAR)
        argTypes = new Type[] {TINYINT_ARRAY, VarcharType.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(VarcharType.VARCHAR, fn.getArgs()[1]);

        // array_append(NULL, NULL)
        argTypes = new Type[] {NullType.NULL, NullType.NULL};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(new ArrayType(BooleanType.BOOLEAN), fn.getReturnType());
        Assertions.assertEquals(new ArrayType(BooleanType.BOOLEAN), fn.getArgs()[0]);

        // array_append(ARRAY<ARRAY<INT>>, ARRAY<INT>)
        argTypes = new Type[] {INT_ARRAY_ARRAY, INT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<INT>>, ARRAY<TINYINT>)
        argTypes = new Type[] {INT_ARRAY_ARRAY, TINYINT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<TINYINT>>, ARRAY<INT>)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, INT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<TINYINT>>, NULL)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, NullType.NULL};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(TINYINT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(TINYINT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(TINYINT_ARRAY, fn.getArgs()[1]);

        // array_append(NULL, ARRAY<INT>)
        argTypes = new Type[] {NullType.NULL, INT_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<ARRAY<TINYINT>>, TINYINT)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, IntegerType.TINYINT};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_append(ARRAY<ARRAY<TINYINT>>, ARRAY<VARCHAR>)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY, VARCHAR_ARRAY};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(new ArrayType(VARCHAR_ARRAY), fn.getReturnType());
        Assertions.assertEquals(new ArrayType(VARCHAR_ARRAY), fn.getArgs()[0]);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getArgs()[1]);

        // array_append(ARRAY<VARCHAR>, VARCHAR)
        argTypes = new Type[] {VARCHAR_ARRAY, VarcharType.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(VarcharType.VARCHAR, fn.getArgs()[1]);

        // array_append(ARRAY<VARCHAR>, CHAR)
        argTypes = new Type[] {VARCHAR_ARRAY, CharType.CHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getArgs()[0]);
        Assertions.assertEquals(VarcharType.VARCHAR, fn.getArgs()[1]);

        // array_append(VARCHAR, VARCHAR)
        argTypes = new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_append(INT, VARCHAR)
        argTypes = new Type[] {IntegerType.INT, VarcharType.VARCHAR};
        desc = new Function(new FunctionName("array_append"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_length(INT)
        argTypes = new Type[] {IntegerType.INT};
        desc = new Function(new FunctionName("array_length"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_length(INT)
        argTypes = new Type[] {IntegerType.INT};
        desc = new Function(new FunctionName("array_length"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNull(fn);

        // array_length(ARRAY<INT>)
        argTypes = new Type[] {INT_ARRAY};
        desc = new Function(new FunctionName("array_length"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(IntegerType.INT, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);

        // array_length(ARRAY<ARRAY<INT>>)
        argTypes = new Type[] {INT_ARRAY_ARRAY};
        desc = new Function(new FunctionName("array_length"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(IntegerType.INT, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);

        // array_length(NULL)
        argTypes = new Type[] {NullType.NULL};
        desc = new Function(new FunctionName("array_length"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(IntegerType.INT, fn.getReturnType());
        Assertions.assertEquals(new ArrayType(BooleanType.BOOLEAN), fn.getArgs()[0]);

        // array_generate(SmallInt,Int,BigInt)
        argTypes = new Type[] {IntegerType.SMALLINT, IntegerType.INT, IntegerType.BIGINT};
        desc = new Function(new FunctionName("array_generate"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(ArrayType.ARRAY_BIGINT, fn.getReturnType());
        Assertions.assertEquals(IntegerType.BIGINT, fn.getArgs()[0]);

        // arrays_overlap
        argTypes = new Type[] {ArrayType.ARRAY_BIGINT, ArrayType.ARRAY_TINYINT};
        desc = new Function(new FunctionName("arrays_overlap"), argTypes, BooleanType.BOOLEAN, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(fn.functionId, 150216L);

        // array_flatten(ARRAY<ARRAY<TINYINT>>)
        argTypes = new Type[] {TINYINT_ARRAY_ARRAY};
        desc = new Function(new FunctionName("array_flatten"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(TINYINT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(TINYINT_ARRAY_ARRAY, fn.getArgs()[0]);

        // array_flatten(ARRAY<ARRAY<INT>>)
        argTypes = new Type[] {INT_ARRAY_ARRAY};
        desc = new Function(new FunctionName("array_flatten"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(INT_ARRAY, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);

        // array_flatten(ARRAY<ARRAY<INT>>)
        argTypes = new Type[] {VARCHAR_ARRAY_ARRAY};
        desc = new Function(new FunctionName("array_flatten"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(VARCHAR_ARRAY, fn.getReturnType());
        Assertions.assertEquals(VARCHAR_ARRAY_ARRAY, fn.getArgs()[0]);

        // null_or_empty(null)
        argTypes = new Type[] {NullType.NULL};
        desc = new Function(new FunctionName("null_or_empty"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(BooleanType.BOOLEAN, fn.getReturnType());
        Assertions.assertEquals(VarcharType.VARCHAR, fn.getArgs()[0]);

        // null_or_empty(ARRAY<INT>)
        argTypes = new Type[] {INT_ARRAY};
        desc = new Function(new FunctionName("null_or_empty"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(BooleanType.BOOLEAN, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY, fn.getArgs()[0]);

        // null_or_empty(ARRAY<ARRAY<INT>>)
        argTypes = new Type[] {INT_ARRAY_ARRAY};
        desc = new Function(new FunctionName("null_or_empty"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(fn);
        Assertions.assertEquals(BooleanType.BOOLEAN, fn.getReturnType());
        Assertions.assertEquals(INT_ARRAY_ARRAY, fn.getArgs()[0]);

        // coalesce
        argTypes = new Type[] {INT_ARRAY_ARRAY, DOUBLE_ARRAY};
        desc = new Function(new FunctionName("coalesce"), argTypes, InvalidType.INVALID, false);
        try {
            functionSet.getFunction(desc, Function.CompareMode.IS_SUPERTYPE_OF);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof SemanticException);
            Assertions.assertTrue(e.getMessage().contains("in the function [coalesce]"));
        }
    }

    @Test
    public void testPolymorphicTVF() {
        // First two columns of the result are polymorphic (derived from argument type), but the last argument is of a
        // "concrete" type BIGINT, which is retained in the resolved function.
        TableFunction polymorphicTVF =
                new TableFunction(new FunctionName("three_column_tvf"), Lists.newArrayList("a", "b", "c"),
                        Lists.newArrayList(AnyArrayType.ANY_ARRAY),
                        Lists.newArrayList(AnyElementType.ANY_ELEMENT, AnyElementType.ANY_ELEMENT, IntegerType.BIGINT));

        functionSet.addBuiltin(polymorphicTVF);

        Type[] argTypes = new Type[] {VARCHAR_ARRAY};
        Function desc = new Function(new FunctionName("three_column_tvf"), argTypes, InvalidType.INVALID, false);
        Function fn = functionSet.getFunction(desc, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(fn);
        Assertions.assertTrue(fn instanceof TableFunction);
        TableFunction tableFunction = (TableFunction) fn;
        Assertions.assertEquals(3, tableFunction.getTableFnReturnTypes().size());
        Assertions.assertEquals(VarcharType.VARCHAR, tableFunction.getTableFnReturnTypes().get(0));
        Assertions.assertEquals(VarcharType.VARCHAR, tableFunction.getTableFnReturnTypes().get(1));
        Assertions.assertEquals(IntegerType.BIGINT, tableFunction.getTableFnReturnTypes().get(2));

        // Same but for two column TVF.
        TableFunction twoColumnTVF =
                new TableFunction(new FunctionName("two_column_tvf"), Lists.newArrayList("a", "b"),
                        Lists.newArrayList(AnyArrayType.ANY_ARRAY),
                        Lists.newArrayList(IntegerType.BIGINT, AnyElementType.ANY_ELEMENT));
        functionSet.addBuiltin(twoColumnTVF);

        argTypes = new Type[] {VARCHAR_ARRAY};
        desc = new Function(new FunctionName("two_column_tvf"), argTypes, InvalidType.INVALID, false);
        fn = functionSet.getFunction(desc, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(fn);
        Assertions.assertTrue(fn instanceof TableFunction);
        tableFunction = (TableFunction) fn;
        Assertions.assertEquals(2, tableFunction.getTableFnReturnTypes().size());
        Assertions.assertEquals(IntegerType.BIGINT, tableFunction.getTableFnReturnTypes().get(0));
        Assertions.assertEquals(VarcharType.VARCHAR, tableFunction.getTableFnReturnTypes().get(1));
    }

    // getFunction as it filtered the overloads of a name on every call.
    private Function getFunctionByFiltering(Map<String, List<Function>> byName, Function desc,
                                            Function.CompareMode mode) throws Exception {
        List<Function> fns = byName.get(desc.functionName());
        if (desc.hasNamedArg() && fns != null && !fns.isEmpty()) {
            fns = fns.stream().filter(Function::hasNamedArg).collect(Collectors.toList());
        }
        if (fns == null || fns.isEmpty()) {
            return null;
        }
        List<Function> standFns = fns.stream().filter(fn -> !fn.isPolymorphic()).collect(Collectors.toList());
        Function func = invokeMatcher("matchStrictFunction", desc, mode, standFns);
        if (func != null) {
            return func;
        }
        List<Function> polyFns = fns.stream().filter(Function::isPolymorphic).collect(Collectors.toList());
        func = invokeMatcher("matchPolymorphicFunction", desc, mode, polyFns, standFns);
        if (func != null) {
            return func;
        }
        return invokeMatcher("matchCastFunction", desc, mode, standFns);
    }

    private Function invokeMatcher(String name, Object... args) throws Exception {
        Class<?>[] types = new Class<?>[args.length];
        types[0] = Function.class;
        types[1] = Function.CompareMode.class;
        for (int i = 2; i < args.length; i++) {
            types[i] = List.class;
        }
        Method method = FunctionSet.class.getDeclaredMethod(name, types);
        method.setAccessible(true);
        try {
            return (Function) method.invoke(functionSet, args);
        } catch (InvocationTargetException e) {
            throw (Exception) e.getCause();
        }
    }

    private static Type concretize(Type type) {
        if (!type.isPseudoType()) {
            return type;
        }
        if (type instanceof AnyArrayType) {
            return INT_ARRAY;
        }
        if (type instanceof AnyMapType) {
            return new MapType(IntegerType.INT, IntegerType.INT);
        }
        return IntegerType.INT;
    }

    private static Type widen(Type type) {
        if (type.isFixedPointType()) {
            return IntegerType.BIGINT;
        }
        if (type.isStringType()) {
            return VarcharType.VARCHAR;
        }
        return type;
    }

    private static Function.CompareMode[] allModes() {
        return Function.CompareMode.values();
    }

    private static String describe(Function fn) {
        if (fn == null) {
            return "null";
        }
        return fn.getClass().getSimpleName() + " " + fn.signatureString() + " -> " + fn.getReturnType()
                + " polymorphic=" + fn.isPolymorphic();
    }

    @Test
    public void testGetFunctionMatchesPerCallFiltering() throws Exception {
        Map<String, List<Function>> byName = Maps.newLinkedHashMap();
        for (Function fn : functionSet.getBuiltinFunctions()) {
            byName.computeIfAbsent(fn.functionName(), k -> Lists.newArrayList()).add(fn);
        }
        Assertions.assertTrue(byName.size() > 500);
        Set<Function> registered = Collections.newSetFromMap(new IdentityHashMap<>());
        registered.addAll(functionSet.getBuiltinFunctions());

        List<Function> descs = new ArrayList<>();
        for (Map.Entry<String, List<Function>> entry : byName.entrySet()) {
            FunctionName name = new FunctionName(entry.getKey());
            for (Function fn : entry.getValue()) {
                Type[] args = fn.getArgs();
                Type[] concrete = new Type[args.length];
                Type[] widened = new Type[args.length];
                Type[] nulls = new Type[args.length];
                for (int i = 0; i < args.length; i++) {
                    concrete[i] = concretize(args[i]);
                    widened[i] = widen(concrete[i]);
                    nulls[i] = NullType.NULL;
                }
                boolean varArgs = fn.hasVarArgs();
                descs.add(new Function(name, args, InvalidType.INVALID, varArgs));
                descs.add(new Function(name, concrete, InvalidType.INVALID, varArgs));
                descs.add(new Function(name, widened, InvalidType.INVALID, varArgs));
                descs.add(new Function(name, nulls, InvalidType.INVALID, varArgs));
                descs.add(new Function(name, concrete, InvalidType.INVALID, !varArgs));
                if (args.length > 0) {
                    Type[] shorter = Arrays.copyOf(concrete, args.length - 1);
                    descs.add(new Function(name, shorter, InvalidType.INVALID, varArgs));
                }
                Type[] longer = Arrays.copyOf(concrete, args.length + 1);
                longer[args.length] = IntegerType.INT;
                descs.add(new Function(name, longer, InvalidType.INVALID, varArgs));

                if (fn.hasNamedArg()) {
                    String[] names = fn.getArgNames();
                    descs.add(new Function(name, concrete, names, InvalidType.INVALID, varArgs));
                    descs.add(new Function(name, widened, names, InvalidType.INVALID, varArgs));
                    if (names.length > 1) {
                        descs.add(new Function(name, Arrays.copyOf(concrete, names.length - 1),
                                Arrays.copyOf(names, names.length - 1), InvalidType.INVALID, varArgs));
                    }
                } else if (args.length > 0) {
                    // named arguments on a call to a name whose overload has none
                    String[] names = new String[args.length];
                    for (int i = 0; i < names.length; i++) {
                        names[i] = "arg" + i;
                    }
                    descs.add(new Function(name, concrete, names, InvalidType.INVALID, varArgs));
                }
            }
        }
        descs.add(new Function(new FunctionName("no_such_function"), new Type[] {IntegerType.INT},
                InvalidType.INVALID, false));
        descs.add(new Function(new FunctionName("no_such_function"), new Type[] {IntegerType.INT},
                new String[] {"a"}, InvalidType.INVALID, false));

        int compared = 0;
        int nonNull = 0;
        for (Function desc : descs) {
            for (Function.CompareMode mode : allModes()) {
                Function expected = null;
                Function actual = null;
                Throwable expectedError = null;
                Throwable actualError = null;
                try {
                    expected = getFunctionByFiltering(byName, desc, mode);
                } catch (Throwable e) {
                    expectedError = e;
                }
                try {
                    actual = functionSet.getFunction(desc, mode);
                } catch (Throwable e) {
                    actualError = e;
                }
                String where = desc.functionName() + desc.signatureString() + " named=" + desc.hasNamedArg()
                        + " mode=" + mode;
                if (expectedError != null || actualError != null) {
                    Assertions.assertNotNull(expectedError, where);
                    Assertions.assertNotNull(actualError, where);
                    Assertions.assertEquals(expectedError.getClass(), actualError.getClass(), where);
                    Assertions.assertEquals(expectedError.getMessage(), actualError.getMessage(), where);
                } else if (expected == null || actual == null) {
                    Assertions.assertNull(expected, where + " expected " + describe(expected));
                    Assertions.assertNull(actual, where + " actual " + describe(actual));
                } else {
                    // a registered overload must come back as the same object; a generated one as an equal copy
                    if (registered.contains(expected)) {
                        Assertions.assertSame(expected, actual, where);
                    }
                    Assertions.assertEquals(expected, actual, where);
                    Assertions.assertEquals(describe(expected), describe(actual), where);
                    nonNull++;
                }
                compared++;
            }
        }
        Assertions.assertTrue(compared > 10000);
        Assertions.assertTrue(nonNull > 5000);
    }

    @Test
    public void testGetFunctionSeesOverloadsAddedLater() {
        FunctionName name = new FunctionName("late_added_tvf");
        Function intArray = new Function(name, new Type[] {INT_ARRAY}, InvalidType.INVALID, false);
        Function varcharArray = new Function(name, new Type[] {VARCHAR_ARRAY}, InvalidType.INVALID, false);
        Assertions.assertNull(functionSet.getFunction(intArray, Function.CompareMode.IS_SUPERTYPE_OF));

        TableFunction stand = new TableFunction(name, Lists.newArrayList("a"),
                Lists.newArrayList(INT_ARRAY), Lists.newArrayList(IntegerType.BIGINT));
        functionSet.addBuiltin(stand);
        Assertions.assertSame(stand, functionSet.getFunction(intArray, Function.CompareMode.IS_IDENTICAL));
        Assertions.assertNull(functionSet.getFunction(varcharArray, Function.CompareMode.IS_SUPERTYPE_OF));

        TableFunction poly = new TableFunction(name, Lists.newArrayList("a"),
                Lists.newArrayList(AnyArrayType.ANY_ARRAY), Lists.newArrayList(AnyElementType.ANY_ELEMENT));
        functionSet.addBuiltin(poly);
        Assertions.assertSame(stand, functionSet.getFunction(intArray, Function.CompareMode.IS_IDENTICAL));
        Function generated = functionSet.getFunction(varcharArray, Function.CompareMode.IS_SUPERTYPE_OF);
        Assertions.assertNotNull(generated);
        Assertions.assertEquals(VarcharType.VARCHAR, ((TableFunction) generated).getTableFnReturnTypes().get(0));
    }
}
