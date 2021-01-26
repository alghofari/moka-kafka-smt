package com.moka.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.time.Instant;
import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class TimestamptzConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC;
    static {
        OVERVIEW_DOC = "Convert string timestamp with timezone to epoch time in specified fields."
                + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName()
                + "</code>) or value (<code>" + Value.class.getName() + "</code>).";
    }

    public static final String FIELDS_CONFIG = "fields";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, "fields", new NonEmptyListValidator(),
                    ConfigDef.Importance.MEDIUM, "Names of fields to remove timezone");

    private static final String PURPOSE = "convert string timestamp with timezone to epoch time in specified fields";

    private Set<String> removeTimeZoneFields;
    private Cache<Schema, Schema> schemaUpdateCache;

    private static final Schema OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().build();

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        removeTimeZoneFields = new HashSet<>(config.getList(FIELDS_CONFIG));
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        }
        else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final HashMap<String, Object> updatedValue = new HashMap<>(value);
        for (String field : removeTimeZoneFields) {
            final Object fieldValue = value.get(field);
            updatedValue.put(field, parsingTimestamp(fieldValue));
        }
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        Schema valueSchema = operatingSchema(record);
        Schema updatedSchema = getOrBuildSchema(valueSchema);

        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object origFieldValue = value.get(field);
            final Object newFieldValue = removeTimeZoneFields.contains(field.name()) && origFieldValue != null ? parsingTimestamp(origFieldValue) : origFieldValue;
            updatedValue.put(updatedSchema.field(field.name()), newFieldValue);
        }
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema getOrBuildSchema(Schema valueSchema) {
        Schema updatedSchema = schemaUpdateCache.get(valueSchema);
        if (updatedSchema != null)
            return updatedSchema;

        final SchemaBuilder builder;
        builder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());
        for (Field field : valueSchema.fields()) {
            if (removeTimeZoneFields.contains(field.name())) {
                builder.field(field.name(), OPTIONAL_TIMESTAMP_SCHEMA);
            }
            else {
                builder.field(field.name(), field.schema());
            }
        }

        updatedSchema = builder.build();
        schemaUpdateCache.put(valueSchema, updatedSchema);
        return updatedSchema;
    }

    private java.sql.Timestamp parsingTimestamp(Object value) {
        Instant i = Instant.parse(value.toString());
        return java.sql.Timestamp.from(i);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends TimestamptzConverter<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends TimestamptzConverter<R> {
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