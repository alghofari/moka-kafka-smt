# Kafka Connect Custom Transformations
Single Message Transformations (SMTs) are applied to messages as they flow through Connect. SMTs transform inbound messages after a source connector has produced them, but before they are written to Kafka. The following SMTs are available for use with Kafka Connect.

| Transform | Description |
|--|--|
| CoalesceField | Replace null object value with valid null |
| InsertTimestamp | Insert epoch timestamp field |
| RemoveField | Remove specified fields if value null |
| TimestamptzConverter | Convert string timestamp with a timezone to epoch time |
| WrapField | Wrap data using the specified field name in a single string |

## CoalesceField
The following provides usage information for the Moka Kafka SMT `com.moka.kafka.connect.smt.CoalesceField`
### Description
Coalesce specified fields with a valid null value for the field type (i.e. 0, false, empty string, and so on).
Use the concrete transformation type designed for the record key (`com.moka.kafka.connect.smt.CoalesceField$Key`) or value (`com.moka.kafka.connect.smt.CoalesceField$Value`).
### Example
This configuration snippet shows how to use `CoalesceField` to coalesce the value of a field.
```
"transforms": "CoalesceField",
"transforms.CoalesceField.type": "com.moka.kafka.connect.smt.CoalesceField$Value",
"transforms.CoalesceField.fields": "string_field"
```
This coalesces `string_field`, transforming the original message as seen here:
```
{"integer_field":22, "string_field":null}
```
into the result here:
```
{"integer_field":22, "string_field":""}
```
### Properties
| Name | Description | Type | Default | Valid Values | Importance
|--|--|--|--|--|--|
| `fields` | Names of fields to coalesce. | list |  | non-empty list | high |

## InsertTimestamp
The following provides usage information for the Moka Kafka SMT `com.moka.kafka.connect.smt.InsertTimestamp`.
### Description
Insert epoch timestamp into a record.
Use the concrete transformation type designed for the record key (`com.moka.kafka.connect.smt.InsertTimestamp$Key`) or value (`com.moka.kafka.connect.smt.InsertTimestamp$Value`).
### Example
This configuration snippet shows how to use `InsertTimestamp` to insert epoch timestamp field labeled `event_timestamp`.
```
"transforms": "InsertTimestamp",
"transforms.InsertTimestamp.type": "com.moka.kafka.connect.smt.InsertTimestamp$Value",
"transforms.InsertTimestamp.field": "event_timestamp"
```
Before: 
```
{"author": "Philip K. Dick", "character": "Palmer Eldritch"}
```
After:
```
{"author": "Philip K. Dick", "character": "Palmer Eldritch", "event_timestamp": 1598962063}
```
### Properties
| Name | Description | Type | Default | Valid Values | Importance
|--|--|--|--|--|--|
| `field` | Field names for epoch timestamp. | string |  |  | medium |

## RemoveField
The following provides usage information for the Moka Kafka SMT `com.moka.kafka.connect.smt.RemoveField`.
### Description
Remove specified fields if value is null.
Use the concrete transformation type designed for the record key (`com.moka.kafka.connect.smt.RemoveField$Key`) or value (`com.moka.kafka.connect.smt.RemoveField$Value`).
### Example
This configuration snippet shows how to use `RemoveField` to remove the field if value is null
```
"transforms": "RemoveField",
"transforms.RemoveField.type": "com.moka.kafka.connect.smt.RemoveField$Value"
```
Messages sent:
```
{
   "author": "Philip K. Dick",
   "character": null
}
```
Topic result:
```
{
   "author": "Philip K. Dick"
}
```
### Properties
| Name | Description | Type | Default | Valid Values | Importance
|--|--|--|--|--|--|
| `fields` | Names of fields to remove if value is null. | list |  |  | medium |

## WrapField
The following provides usage information for the Moka Kafka SMT `com.moka.kafka.connect.smt.WrapField`.
### Description
Wrap data using the specified field name in a single string.
Use the concrete transformation type designed for the record key (`com.moka.kafka.connect.smt.WrapField$Key`) or value (`com.moka.kafka.connect.smt.WrapField$Value`).
### Example
This configuration snippet shows how to use `WrapField` to wrap the field name in a string
```
"transforms": "WrapField",
"transforms.WrapField.type": "com.moka.kafka.connect.smt.WrapField$Value",
"transforms.WrapField.field": "payload"
```
Messages sent:
```
{
   "author": "Philip K. Dick",
   "character": "Palmer Eldritch"
}
```
Topic result:
```
{
   "payload": "\"author\": \"Philip K. Dick\", \"character\": \"Palmer Eldritch\""
}
```
### Properties
| Name | Description | Type | Default | Valid Values | Importance
|--|--|--|--|--|--|
| `field` | Field names for the single field that will be created in the resulting string. | string |  |  | medium |

## TimestamptzConverter
The following provides usage information for the Moka Kafka SMT `com.moka.kafka.connect.smt.TimestamptzConverter`.
### Description
Convert string timestamp with a timezone to epoch time in specified fields.
Use the concrete transformation type designed for the record key (`com.moka.kafka.connect.smt.TimestamptzConverter$Key`) or value (`com.moka.kafka.connect.smt.TimestamptzConverter$Value`).
### Example
This configuration snippet shows how to use `TimestamptzConverter` to convert string timestamp with a timezone to epoch time
```
"transforms": "TimestamptzConverter",
"transforms.WrapField.type": "com.moka.kafka.connect.smt.TimestamptzConverter$Value",
"transforms.TimestamptzConverter.fields": "created_at"
```
Messages sent:
```
{
   "created_at": "2020-11-06T12:21:00Z"
}
```
Topic result:
```
{
   "created_at": 1604665260000
}
```
### Properties
| Name | Description | Type | Default | Valid Values | Importance
|--|--|--|--|--|--|
| `fields` | Names of fields to convert string timestamp with a timezone to epoch time. | list |  |  | medium |