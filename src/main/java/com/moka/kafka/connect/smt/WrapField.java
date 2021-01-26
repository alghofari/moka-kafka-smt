package com.moka.kafka.connect.smt;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class WrapField<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC;
    static {
        OVERVIEW_DOC = "Wrap data using the specified field name in a single String data."
                + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
                + "or value (<code>" + Value.class.getName() + "</code>).";
    }

    private static final String FIELD_CONFIG = "field";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM,
                    "Field name for the single field that will be created in the resulting String.");

    private static final String PURPOSE = "wrap fields";

    private Cache<Schema, Schema> schemaUpdateCache;

    private String fieldName;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString("field");
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        final Object value = operatingValue(record);

        if (schema == null) {
            return newRecord(record, null, Collections.singletonMap(fieldName, value));
        }
        else {
            Schema updatedSchema = schemaUpdateCache.get(schema);
            if (updatedSchema == null) {
                updatedSchema = SchemaBuilder.struct().field(fieldName, Schema.STRING_SCHEMA).build();
                schemaUpdateCache.put(schema, updatedSchema);
            }

            final String newValue = parseValue(requireStruct(value, PURPOSE));
            final Struct updatedValue = new Struct(updatedSchema).put(fieldName, newValue);

            return newRecord(record, updatedSchema, updatedValue);
        }
    }

    private static String parseValue(Struct value) {
        final ObjectMapper mapper = new ObjectMapper();
        final Map<String, Object> valueMap = new HashMap<>();

        for (Field field : value.schema().fields()) {
            final String origFieldName = field.name();
            final Object origFieldValue = value.get(field);
            valueMap.put(origFieldName, origFieldValue);
        }

        try {
            return mapper.writeValueAsString(valueMap);
        }
        catch (Exception e) {
            throw new ConfigException("Convert map to json failed");
        }
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends WrapField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends WrapField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

}
