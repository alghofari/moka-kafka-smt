package com.moka.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class TimestamptzConverterTest {

    @Test
    public void testWithSchema() {
        final TimestamptzConverter<SourceRecord> xformValue = new TimestamptzConverter.Value<>();
        xformValue.configure(Collections.singletonMap(TimestamptzConverter.FIELDS_CONFIG, "created_at"));

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("created_at", Schema.STRING_SCHEMA);
        Schema supportedTypesSchema = builder.build();

        Struct recordValue = new Struct(supportedTypesSchema);
        recordValue.put("created_at", "2020-11-06T12:21:00.23Z");

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
                supportedTypesSchema, recordValue));

        Schema transformedSchema = ((Struct) transformed.value()).schema();
        assertEquals(Schema.OPTIONAL_INT64_SCHEMA.type(), transformedSchema.field("created_at").schema().type());
    }

}
