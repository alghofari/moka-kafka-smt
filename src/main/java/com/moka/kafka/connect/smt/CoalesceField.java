package com.moka.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;
import java.util.Date;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class CoalesceField<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC;
    static {
        OVERVIEW_DOC = "Coalesce specified fields with a valid null value for the field type."
                + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName()
                + "</code>) or value (<code>" + Value.class.getName() + "</code>).";
    }

    private static final String FIELDS_CONFIG = "fields";
    private static final String TYPES_CONFIG = "types";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, "field", new NonEmptyListValidator(),
                    ConfigDef.Importance.MEDIUM, "Names of fields to coalesce")
            .define(TYPES_CONFIG, ConfigDef.Type.LIST, "int32", new NonEmptyListValidator(),
                    ConfigDef.Importance.MEDIUM, "Data type of fields to coalesce");

    private static final String PURPOSE = "coalesce fields";

    private Set<String> coalesceFields;
    private Set<String> types;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        coalesceFields = new HashSet<>(config.getList(FIELDS_CONFIG));
        types = new HashSet<>(config.getList(TYPES_CONFIG));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final HashMap<String, Object> updatedValue = new HashMap<>(value);
        int i = 0;
        for (String field : coalesceFields) {
            String type = types.toArray()[i].toString();
            updatedValue.put(field, coalesce(value.get(field), type, null));
            i++;
        }
        return newRecord(record, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Struct updatedValue = new Struct(value.schema());
        for (Field field : value.schema().fields()) {
            final Object fieldValue = value.get(field);
            final String fieldType = field.schema().type().toString();
            final Schema fieldSchema = field.schema();
            updatedValue.put(field, coalesce(fieldValue, fieldType, fieldSchema));
        }
        return newRecord(record, updatedValue);
    }

    private Object coalesce(Object value, String type, Schema schema) {
        if (value == null) {
            if (schema == null)
                return parseFieldSchemaless(type);
            else
                return parseFieldWithSchema(type, schema);
        }
        else {
            return value;
        }
    }

    private static Object parseFieldSchemaless(String type) {
        Schema.Type targetType = Schema.Type.valueOf(type.trim().toUpperCase(Locale.ROOT));
        if (targetType == Schema.Type.BOOLEAN)
            return Boolean.FALSE;
        else if (targetType == Schema.Type.FLOAT32)
            return 0.0;
        else if (targetType == Schema.Type.FLOAT64)
            return 0.0;
        else if (targetType == Schema.Type.INT8)
            return 0;
        else if (targetType == Schema.Type.INT16)
            return 0;
        else if (targetType == Schema.Type.INT32)
            return 0;
        else if (targetType == Schema.Type.INT64)
            return (long) 0;
        else if (targetType == Schema.Type.STRING)
            return "";
        else
            throw new DataException("Unexpected type in coalesce transformation: " + type);
    }

    private static Object parseFieldWithSchema(String type, Schema schema) {
        Schema.Type targetType = Schema.Type.valueOf(type.trim().toUpperCase(Locale.ROOT));
        if (targetType == Schema.Type.BOOLEAN)
            return Boolean.FALSE;
        else if (targetType == Schema.Type.FLOAT32)
            return 0.0;
        else if (targetType == Schema.Type.FLOAT64)
            return 0.0;
        else if (targetType == Schema.Type.INT8)
            return 0;
        else if (targetType == Schema.Type.INT16)
            return 0;
        else if (targetType == Schema.Type.INT32)
            return 0;
        else if (targetType == Schema.Type.INT64 && schema.name() == null)
            return (long) 0;
        else if (targetType == Schema.Type.INT64 && schema.name().equals(Time.LOGICAL_NAME))
            return new Date(0);
        else if (targetType == Schema.Type.STRING)
            return "";
        else
            throw new DataException("Unexpected type in coalesce transformation: " + type);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R base, Object value);

    public static final class Key<R extends ConnectRecord<R>> extends CoalesceField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends CoalesceField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }
    }

}