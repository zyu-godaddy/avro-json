

# Avro Json decoder&resolver without writer's schema

Avro's [Json encoding](https://avro.apache.org/docs/1.12.0/specification/#json-encoding)
retains enough information of writer's schema such that
it is possible to decode&resolve without writer's schema;
only reader's schema is needed.

[AutoJsonDecoder](./src/main/java/org/example/AutoJsonDecoder.java) 
is a decoder&resolver using reader's schema only.

Given a Json encoded with `writersSchema`, if
- standard decoding with ``writersSchema`` and resolution with `readersSchema` successfully yields datum `d1`
- `AutoJsonDecoder` with `readersSchema` successfully yields datum `d2`

we claim that `d2=d1` (with caveats*), irrespective of `writersSchema`.

Define following rules of `(avro-schema, json-value) -> datum`:

1. `(null, null) -> null`

2. `(boolean, boolean) -> boolean`

3. `(int/long/float/double, number) -> number`

4. `(string, string) -> string`

5. `(bytes/fixed, string) ->` ISO_8859_1 encoding of the string (caveat*)

6. `(enum, string) ->` <br/>the enum symbol matching the string, or the default value of the enum.

7. `(array[T], array)` <br/>apply rule `(T,v)` on each element of the json array.

8. `(map[T], object)` <br/>apply rule `(T,v)` on each property of the json object.

9. `(record, object)` <br/>for each record field `f` of type `T`,
   apply rule `(T,v)` on the object property with the same name, or yield the default value of `f`

10. `(union, {t:v})` <br/>apply rule `(T,v)`, where `T` with name `t` is in the union type;
    or find the first `T` in union that rule `(T,v)` succeeds.

11. `(T, {t:v})` // `T` is not a union<br/>apply rule `#10` with `(union[T], {t:v})`.

12. `(union, j)` // for any `j` <br/>find the first `T` in union that rule `(T,j)` succeeds.

`AutoJsonDecoder` applies rules recursively on `(readersSchema, jsonNode)`.
Ambiguities exist
which could produce multiple interpretations at different levels.
Decoding succeeds if there is a single
interpretation at the root level.

Ambiguities exist because of rule `#11` and `#12`, to handle the possibility
that one of the w/r schemas is a union and the other is not.
These rules can be turned off if justified by the use case.

## usage

`AutoJsonDecoder` can be useful when writer's schema isn't available.

- use reader's schema anywhere writer's schema is expected,
  for example in `new GenericDatumReader`.

- use `AutoJsonDecoder` to replace `JsonDecoder`.

Example: [TestAutoJsonDecoder](src/test/java/org/example/TestAutoJsonDecoder.java)

## caveats

While `string` and `bytes` can be promoted to each other in schema resolution,
the spec doesn't say which charset encoding should be used.
This implementation uses ISO_8859_1 (by necessity).

We only promise that if decoding&resolving succeeds *both*
in the standard way *and* by `AutoJsonDecoder`,
the results will be the same.
Otherwise, false negatives and false positives are possible.

