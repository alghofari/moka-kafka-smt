package com.moka.kafka.connect.smt;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class WrapFieldTest {

    private final WrapField<SinkRecord> xform = new WrapField.Key<>();

    private static final Schema SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("comment", Schema.STRING_SCHEMA)
            .field("amount", Schema.FLOAT32_SCHEMA)
            .field("is_comment", Schema.BOOLEAN_SCHEMA)
            .build();
    private static final Struct KEY_WITH_SCHEMA = new Struct(SCHEMA);
    static {
        KEY_WITH_SCHEMA.put("id", 42);
        KEY_WITH_SCHEMA.put("comment", "good");
        KEY_WITH_SCHEMA.put("amount", 42f);
        KEY_WITH_SCHEMA.put("is_comment", Boolean.TRUE);
    }

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaless() {
        xform.configure(Collections.singletonMap("field", "magic"));

        final SinkRecord record = new SinkRecord("test", 0, null, 42, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.keySchema());
        assertEquals(Collections.singletonMap("magic", 42), transformedRecord.key());
    }

    @Test
    public void withSchema() {
        xform.configure(Collections.singletonMap("field", "magic"));

        final SinkRecord record = new SinkRecord("test", 0, SCHEMA, KEY_WITH_SCHEMA, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Map<String, Object> valueMap = new HashMap<>();
        for (Field field : KEY_WITH_SCHEMA.schema().fields()) {
            final String origFieldName = field.name();
            final Object origFieldValue = KEY_WITH_SCHEMA.get(field);
            valueMap.put(origFieldName, origFieldValue);
        }

        final ObjectMapper mapper = new ObjectMapper();
        String result;
        try {
            result = mapper.writeValueAsString(valueMap);
        }
        catch (Exception e) {
            throw new ConfigException("Convert map to json failed");
        }

        assertEquals(Schema.Type.STRUCT, transformedRecord.keySchema().type());
        assertEquals(Schema.STRING_SCHEMA,  transformedRecord.keySchema().field("magic").schema());
        assertEquals(result, ((Struct) transformedRecord.key()).get("magic"));
    }

}
