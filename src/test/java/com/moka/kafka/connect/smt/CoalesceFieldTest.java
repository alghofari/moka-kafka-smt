package com.moka.kafka.connect.smt;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.*;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class CoalesceFieldTest {

    private static final Schema SCHEMA = SchemaBuilder.struct()
            .field("bool", Schema.BOOLEAN_SCHEMA)
            .field("byte", Schema.INT8_SCHEMA)
            .field("short", Schema.INT16_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .field("timestamp", Timestamp.SCHEMA)
            .build();

    private static final Map<String, Object> VALUES = new HashMap<>();
    private static final Map<String, Object> VALUES_NULL = new HashMap<>();
    private static final Map<String, Object> TYPES = new HashMap<>();
    private static final Struct VALUES_WITH_SCHEMA = new Struct(SCHEMA);

    static {
        VALUES.put("bool", true);
        VALUES.put("byte", (byte) 42);
        VALUES.put("short", (short) 42);
        VALUES.put("int", 42);
        VALUES.put("long", 42L);
        VALUES.put("float", 42f);
        VALUES.put("double", 42d);
        VALUES.put("string", "55.121.20.20");

        VALUES_NULL.put("bool", null);
        VALUES_NULL.put("byte", null);
        VALUES_NULL.put("short", null);
        VALUES_NULL.put("int", null);
        VALUES_NULL.put("long", null);
        VALUES_NULL.put("float", null);
        VALUES_NULL.put("double", null);
        VALUES_NULL.put("string", null);

        TYPES.put("boolean", "bool");
        TYPES.put("int8", "byte");
        TYPES.put("int16", "short");
        TYPES.put("int32", "int");
        TYPES.put("int64", "long");
        TYPES.put("float32", "float");
        TYPES.put("float64", "double");
        TYPES.put("string", "string");

        VALUES_WITH_SCHEMA.put("bool", true);
        VALUES_WITH_SCHEMA.put("byte", (byte) 42);
        VALUES_WITH_SCHEMA.put("short", (short) 42);
        VALUES_WITH_SCHEMA.put("int", 42);
        VALUES_WITH_SCHEMA.put("long", 42L);
        VALUES_WITH_SCHEMA.put("float", 42f);
        VALUES_WITH_SCHEMA.put("double", 42d);
        VALUES_WITH_SCHEMA.put("string", "hmm");
        VALUES_WITH_SCHEMA.put("timestamp", new Date(1598939366));
    }

    private static CoalesceField<SinkRecord> transform(List<String> fields, List<Object> types) {
        final CoalesceField<SinkRecord> xform = new CoalesceField.Value<>();
        Map<String, Object> props = new HashMap<>();
        props.put("fields", fields);
        props.put("types", types);
        xform.configure(props);
        return xform;
    }

    private static SinkRecord record(Schema schema, Object value) {
        return new SinkRecord("", 0, null, null, schema, value, 0);
    }

    @Test
    public void testSchemaless() {
        final List<String> coalesceFields = new ArrayList<>(VALUES.keySet());
        final List<Object> types = new ArrayList<>(TYPES.keySet());
        @SuppressWarnings({"unchecked", "rawtypes"})
        final Map<String, Object> updatedValue = (Map) transform(coalesceFields, types).apply(record(null, VALUES)).value();

        assertEquals(true, updatedValue.get("bool"));
        assertEquals((byte) 42, updatedValue.get("byte"));
        assertEquals((short) 42, updatedValue.get("short"));
        assertEquals(42, updatedValue.get("int"));
        assertEquals(42L, updatedValue.get("long"));
        assertEquals(42f, updatedValue.get("float"));
        assertEquals(42d, updatedValue.get("double"));
        assertEquals("55.121.20.20", updatedValue.get("string"));
    }

    @Test
    public void testWithSchema() {
        final List<String> coalesceFields = new ArrayList<>(SCHEMA.fields().size());
        final List<Object> types = new ArrayList<>(SCHEMA.fields().size());
        for (Field field : SCHEMA.fields()) {
            coalesceFields.add(field.name());
            types.add(field.schema().type());
        }

        final Struct updatedValue = (Struct) transform(coalesceFields, types).apply(record(SCHEMA, VALUES_WITH_SCHEMA)).value();

        assertEquals(true, updatedValue.get("bool"));
        assertEquals((byte) 42, updatedValue.get("byte"));
        assertEquals((short) 42, updatedValue.get("short"));
        assertEquals(42, updatedValue.get("int"));
        assertEquals(42L, updatedValue.get("long"));
        assertEquals(42f, updatedValue.get("float"));
        assertEquals(42d, updatedValue.get("double"));
        assertEquals("hmm", updatedValue.get("string"));
        assertEquals(new Date(1598939366), updatedValue.get("timestamp"));
    }

    @Test
    public void testNullValueSchemaless() {
        final List<String> coalesceFields = new ArrayList<>(VALUES_NULL.keySet());
        final List<Object> types = new ArrayList<>(TYPES.keySet());
        @SuppressWarnings({"unchecked", "rawtypes"})
        final Map<String, Object> updatedValue = (Map) transform(coalesceFields, types).apply(record(null, VALUES_NULL)).value();

        assertEquals(false, updatedValue.get("bool"));
        assertEquals(0, updatedValue.get("byte"));
        assertEquals(0, updatedValue.get("short"));
        assertEquals(0.0, updatedValue.get("int"));
        assertEquals(0, updatedValue.get("long"));
        assertEquals(0.0, updatedValue.get("float"));
        assertEquals((long) 0, updatedValue.get("double"));
        assertEquals("", updatedValue.get("string"));
    }

}