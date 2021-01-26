package com.moka.kafka.connect.smt;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.*;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class RemoveFieldTest {

    private static final Schema SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("item", Schema.STRING_SCHEMA)
            .field("category", Schema.OPTIONAL_STRING_SCHEMA)
            .field("amount", Schema.FLOAT64_SCHEMA)
            .field("created_at", Timestamp.SCHEMA)
            .build();

    private static final Map<String, Object> VALUES = new HashMap<>();
    private static final Struct VALUES_WITH_SCHEMA = new Struct(SCHEMA);

    static {
        VALUES.put("id", 1);
        VALUES.put("item", "coffee");
        VALUES.put("category", null);
        VALUES.put("amount", (double) 20000);
        VALUES.put("created_at", new Date());

        VALUES_WITH_SCHEMA.put("id", 1);
        VALUES_WITH_SCHEMA.put("item", "milk");
        VALUES_WITH_SCHEMA.put("category", null);
        VALUES_WITH_SCHEMA.put("amount", (double) 10000);
        VALUES_WITH_SCHEMA.put("created_at", new Date());
    }

    private static RemoveField<SinkRecord> transform(List<String> fields) {
        final RemoveField<SinkRecord> xform = new RemoveField.Value<>();
        Map<String, Object> props = new HashMap<>();
        props.put("fields", fields);
        xform.configure(props);
        return xform;
    }

    private static SinkRecord record(Schema schema, Object value) {
        return new SinkRecord("", 0, null, null, schema, value, 0);
    }

    @Test
    public void testSchemaless() {
        final List<String> removeFields = new ArrayList<>(VALUES.keySet());
        @SuppressWarnings({"unchecked", "rawtypes"})
        final Map<String, Object> updatedValue = (Map) transform(removeFields).apply(record(null, VALUES)).value();

        assertEquals(4, updatedValue.size());
    }

    @Test
    public void testWithSchema() {
        final List<String> removeFields = new ArrayList<>(SCHEMA.fields().size());
        for (Field field : SCHEMA.fields()) {
            removeFields.add(field.name());
        }
        final Struct updatedValue = (Struct) transform(removeFields).apply(record(SCHEMA, VALUES_WITH_SCHEMA)).value();

        assertEquals(4, updatedValue.schema().fields().size());
    }

}
