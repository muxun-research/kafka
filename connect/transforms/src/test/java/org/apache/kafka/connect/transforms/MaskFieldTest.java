/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MaskFieldTest {

    private static final Schema SCHEMA = SchemaBuilder.struct().field("magic", Schema.INT32_SCHEMA).field("bool", Schema.BOOLEAN_SCHEMA).field("byte", Schema.INT8_SCHEMA).field("short", Schema.INT16_SCHEMA).field("int", Schema.INT32_SCHEMA).field("long", Schema.INT64_SCHEMA).field("float", Schema.FLOAT32_SCHEMA).field("double", Schema.FLOAT64_SCHEMA).field("string", Schema.STRING_SCHEMA).field("date", org.apache.kafka.connect.data.Date.SCHEMA).field("time", Time.SCHEMA).field("timestamp", Timestamp.SCHEMA).field("decimal", Decimal.schema(0)).field("array", SchemaBuilder.array(Schema.INT32_SCHEMA)).field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)).build();
    private static final Map<String, Object> VALUES = new HashMap<>();
    private static final Struct VALUES_WITH_SCHEMA = new Struct(SCHEMA);

    static {
        VALUES.put("magic", 42);
        VALUES.put("bool", true);
        VALUES.put("byte", (byte) 42);
        VALUES.put("short", (short) 42);
        VALUES.put("int", 42);
        VALUES.put("long", 42L);
        VALUES.put("float", 42f);
        VALUES.put("double", 42d);
        VALUES.put("string", "55.121.20.20");
        VALUES.put("date", new Date());
        VALUES.put("bigint", new BigInteger("42"));
        VALUES.put("bigdec", new BigDecimal("42.0"));
        VALUES.put("list", singletonList(42));
        VALUES.put("map", Collections.singletonMap("key", "value"));

        VALUES_WITH_SCHEMA.put("magic", 42);
        VALUES_WITH_SCHEMA.put("bool", true);
        VALUES_WITH_SCHEMA.put("byte", (byte) 42);
        VALUES_WITH_SCHEMA.put("short", (short) 42);
        VALUES_WITH_SCHEMA.put("int", 42);
        VALUES_WITH_SCHEMA.put("long", 42L);
        VALUES_WITH_SCHEMA.put("float", 42f);
        VALUES_WITH_SCHEMA.put("double", 42d);
        VALUES_WITH_SCHEMA.put("string", "hmm");
        VALUES_WITH_SCHEMA.put("date", new Date());
        VALUES_WITH_SCHEMA.put("time", new Date());
        VALUES_WITH_SCHEMA.put("timestamp", new Date());
        VALUES_WITH_SCHEMA.put("decimal", new BigDecimal(42));
        VALUES_WITH_SCHEMA.put("array", Arrays.asList(1, 2, 3));
        VALUES_WITH_SCHEMA.put("map", Collections.singletonMap("what", "what"));
    }

    private static MaskField<SinkRecord> transform(List<String> fields, String replacement) {
        final MaskField<SinkRecord> xform = new MaskField.Value<>();
        Map<String, Object> props = new HashMap<>();
        props.put("fields", fields);
        props.put("replacement", replacement);
        xform.configure(props);
        return xform;
    }

    private static SinkRecord record(Schema schema, Object value) {
        return new SinkRecord("", 0, null, null, schema, value, 0);
    }

    private static void checkReplacementWithSchema(String maskField, Object replacement) {
        SinkRecord record = record(SCHEMA, VALUES_WITH_SCHEMA);
        final Struct updatedValue = (Struct) transform(singletonList(maskField), String.valueOf(replacement)).apply(record).value();
        assertEquals(replacement, updatedValue.get(maskField), "Invalid replacement for " + maskField + " value");
    }

    private static void checkReplacementSchemaless(String maskField, Object replacement) {
        checkReplacementSchemaless(singletonList(maskField), replacement);
    }

    @SuppressWarnings("unchecked")
    private static void checkReplacementSchemaless(List<String> maskFields, Object replacement) {
        SinkRecord record = record(null, VALUES);
        final Map<String, Object> updatedValue = (Map) transform(maskFields, String.valueOf(replacement)).apply(record).value();
        for (String maskField : maskFields) {
            assertEquals(replacement, updatedValue.get(maskField), "Invalid replacement for " + maskField + " value");
        }
    }

    @Test
    public void testSchemaless() {
        final List<String> maskFields = new ArrayList<>(VALUES.keySet());
        maskFields.remove("magic");
        @SuppressWarnings("unchecked") final Map<String, Object> updatedValue = (Map) transform(maskFields, null).apply(record(null, VALUES)).value();

        assertEquals(42, updatedValue.get("magic"));
        assertEquals(false, updatedValue.get("bool"));
        assertEquals((byte) 0, updatedValue.get("byte"));
        assertEquals((short) 0, updatedValue.get("short"));
        assertEquals(0, updatedValue.get("int"));
        assertEquals(0L, updatedValue.get("long"));
        assertEquals(0f, updatedValue.get("float"));
        assertEquals(0d, updatedValue.get("double"));
        assertEquals("", updatedValue.get("string"));
        assertEquals(new Date(0), updatedValue.get("date"));
        assertEquals(BigInteger.ZERO, updatedValue.get("bigint"));
        assertEquals(BigDecimal.ZERO, updatedValue.get("bigdec"));
        assertEquals(Collections.emptyList(), updatedValue.get("list"));
        assertEquals(Collections.emptyMap(), updatedValue.get("map"));
    }

    @Test
    public void testWithSchema() {
        final List<String> maskFields = new ArrayList<>(SCHEMA.fields().size());
        for (Field field : SCHEMA.fields()) {
            if (!field.name().equals("magic")) {
                maskFields.add(field.name());
            }
        }

        final Struct updatedValue = (Struct) transform(maskFields, null).apply(record(SCHEMA, VALUES_WITH_SCHEMA)).value();

        assertEquals(42, updatedValue.get("magic"));
        assertEquals(false, updatedValue.get("bool"));
        assertEquals((byte) 0, updatedValue.get("byte"));
        assertEquals((short) 0, updatedValue.get("short"));
        assertEquals(0, updatedValue.get("int"));
        assertEquals(0L, updatedValue.get("long"));
        assertEquals(0f, updatedValue.get("float"));
        assertEquals(0d, updatedValue.get("double"));
        assertEquals("", updatedValue.get("string"));
        assertEquals(new Date(0), updatedValue.get("date"));
        assertEquals(new Date(0), updatedValue.get("time"));
        assertEquals(new Date(0), updatedValue.get("timestamp"));
        assertEquals(BigDecimal.ZERO, updatedValue.get("decimal"));
        assertEquals(Collections.emptyList(), updatedValue.get("array"));
        assertEquals(Collections.emptyMap(), updatedValue.get("map"));
    }

    @Test
    public void testSchemalessWithReplacement() {
        checkReplacementSchemaless("short", (short) 123);
        checkReplacementSchemaless("byte", (byte) 123);
        checkReplacementSchemaless("int", 123);
        checkReplacementSchemaless("long", 123L);
        checkReplacementSchemaless("float", 123.0f);
        checkReplacementSchemaless("double", 123.0);
        checkReplacementSchemaless("string", "123");
        checkReplacementSchemaless("bigint", BigInteger.valueOf(123L));
        checkReplacementSchemaless("bigdec", BigDecimal.valueOf(123.0));
    }

    @Test
    public void testSchemalessUnsupportedReplacementType() {
        String exMessage = "Cannot mask value of type";
        Class<DataException> exClass = DataException.class;

        assertThrows(exClass, () -> checkReplacementSchemaless("date", new Date()), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless(Arrays.asList("int", "date"), new Date()), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("bool", false), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("list", singletonList("123")), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("map", Collections.singletonMap("123", "321")), exMessage);
    }

    @Test
    public void testWithSchemaAndReplacement() {
        checkReplacementWithSchema("short", (short) 123);
        checkReplacementWithSchema("byte", (byte) 123);
        checkReplacementWithSchema("int", 123);
        checkReplacementWithSchema("long", 123L);
        checkReplacementWithSchema("float", 123.0f);
        checkReplacementWithSchema("double", 123.0);
        checkReplacementWithSchema("string", "123");
        checkReplacementWithSchema("decimal", BigDecimal.valueOf(123.0));
    }

    @Test
    public void testWithSchemaUnsupportedReplacementType() {
        String exMessage = "Cannot mask value of type";
        Class<DataException> exClass = DataException.class;

        assertThrows(exClass, () -> checkReplacementWithSchema("time", new Date()), exMessage);
        assertThrows(exClass, () -> checkReplacementWithSchema("timestamp", new Date()), exMessage);
        assertThrows(exClass, () -> checkReplacementWithSchema("array", singletonList(123)), exMessage);
    }

    @Test
    public void testReplacementTypeMismatch() {
        String exMessage = "Invalid value  for configuration replacement";
        Class<DataException> exClass = DataException.class;

        assertThrows(exClass, () -> checkReplacementSchemaless("byte", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("short", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("int", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("long", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("float", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("double", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("bigint", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("bigdec", "foo"), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("int", new Date()), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless("int", new Object()), exMessage);
        assertThrows(exClass, () -> checkReplacementSchemaless(Arrays.asList("string", "int"), "foo"), exMessage);
    }

    @Test
    public void testEmptyStringReplacementValue() {
        assertThrows(ConfigException.class, () -> checkReplacementSchemaless("short", ""), "String must be non-empty");
    }

    @Test
    public void testNullListAndMapReplacementsAreMutable() {
        final List<String> maskFields = Arrays.asList("array", "map");
        final Struct updatedValue = (Struct) transform(maskFields, null).apply(record(SCHEMA, VALUES_WITH_SCHEMA)).value();
        @SuppressWarnings("unchecked") List<Integer> actualList = (List<Integer>) updatedValue.get("array");
        assertEquals(Collections.emptyList(), actualList);
        actualList.add(0);
        assertEquals(Collections.singletonList(0), actualList);

        @SuppressWarnings("unchecked") Map<String, String> actualMap = (Map<String, String>) updatedValue.get("map");
        assertEquals(Collections.emptyMap(), actualMap);
        actualMap.put("k", "v");
        assertEquals(Collections.singletonMap("k", "v"), actualMap);
    }
}
