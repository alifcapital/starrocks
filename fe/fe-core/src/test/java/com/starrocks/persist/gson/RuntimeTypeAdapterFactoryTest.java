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

package com.starrocks.persist.gson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.annotations.SerializedName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RuntimeTypeAdapterFactoryTest {
    @Test
    public void test() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapterFactory(RuntimeTypeAdapterFactory
                .of(A.class, "clazz")
                .registerSubtype(B.class, "B")
                .registerSubtype(A.class, "A"));
        Gson gson = builder.create();
        B b = gson.fromJson("{\"a\":1,\"b\":2}", B.class);
        Assertions.assertEquals(1, b.getA());
        Assertions.assertEquals(2, b.getB());

        A a = new A();
        a.a = 10;
        String jsonStr = gson.toJson(a);
        System.out.println(jsonStr);
        A a2 = gson.fromJson(jsonStr, A.class);
        System.out.println(a2.a);
    }

    // Shapes the production subtypes do not have, written by the streaming writer and by the tree-based one.
    private static Gson gson(boolean legacy) {
        GsonBuilder builder = new GsonBuilder();
        if (legacy) {
            builder.registerTypeAdapterFactory(LegacyRuntimeTypeAdapterFactory.of(Shape.class, "clazz")
                    .registerSubtype(Circle.class, "Circle").registerSubtype(Holder.class, "Holder")
                    .registerSubtype(WithTypeField.class, "WithTypeField"));
        } else {
            builder.registerTypeAdapterFactory(RuntimeTypeAdapterFactory.of(Shape.class, "clazz")
                    .registerSubtype(Circle.class, "Circle").registerSubtype(Holder.class, "Holder")
                    .registerSubtype(WithTypeField.class, "WithTypeField"));
        }
        return builder.create();
    }

    // Nested subtypes, nulls, arrays and HTML characters write the same JSON as the tree did.
    @Test
    public void nestedSubtypesWriteTheSameJson() {
        Holder holder = new Holder();
        holder.name = "<a & 'b'>";
        holder.inner = new Circle(1.5);
        holder.shapes = new Shape[] {new Circle(2), null, new Holder()};
        String expected = gson(true).toJson(holder, Shape.class);
        Assertions.assertEquals(expected, gson(false).toJson(holder, Shape.class));
        Assertions.assertTrue(expected.startsWith("{\"clazz\":\"Holder\""), expected);
        Assertions.assertEquals(expected, gson(false).toJson(gson(false).fromJson(expected, Shape.class), Shape.class));
    }

    // As the tree did, a subtype cannot write NaN or a field with the name of the type field.
    @Test
    public void invalidSubtypesFailAsBefore() {
        for (boolean legacy : new boolean[] {true, false}) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> gson(legacy).toJson(new Circle(Double.NaN), Shape.class), "legacy " + legacy);
            Assertions.assertThrows(JsonParseException.class,
                    () -> gson(legacy).toJson(new WithTypeField(), Shape.class), "legacy " + legacy);
        }
    }

    public static class Shape {
    }

    public static class Circle extends Shape {
        @SerializedName("r")
        private double radius;

        public Circle() {
        }

        Circle(double radius) {
            this.radius = radius;
        }
    }

    public static class Holder extends Shape {
        @SerializedName("name")
        private String name;
        @SerializedName("inner")
        private Shape inner;
        @SerializedName("shapes")
        private Shape[] shapes;
        @SerializedName("missing")
        private String missing;
    }

    public static class WithTypeField extends Shape {
        @SerializedName("clazz")
        private String clazz = "mine";
    }

    public static class A {
        @SerializedName("a")
        private int a;

        public int getA() {
            return a;
        }

        public void setA(int a) {
            this.a = a;
        }
    }

    public static class B extends A {
        @SerializedName("b")
        private int b;

        public int getB() {
            return b;
        }

        public void setB(int b) {
            this.b = b;
        }
    }
}
