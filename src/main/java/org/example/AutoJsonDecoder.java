/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.apache.avro.util.internal.Accessor;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * A Json {@link Decoder} and Resolver using reader's schema only.
 * <p>
 *     See {@link #breakAmbiguity(boolean)} to turn off rule#11 and rule#12.
 * </p>
 */
public class AutoJsonDecoder extends Decoder {
    private static JsonFactory jsonFactory = new JsonFactory();
    
    final Schema schema;
    final Callable<JsonNode> nodeSupplier;
    final ArrayList<Object> bin = new ArrayList<>(); // leaf values
    
    boolean breakAmbiguity = false;
    
    /**
     * @param breakAmbiguity If true, turn off rule#11 and rule#12.
     */
    public AutoJsonDecoder breakAmbiguity(boolean breakAmbiguity) {
        this.breakAmbiguity = breakAmbiguity;
        return this;
    }
    
    private AutoJsonDecoder(Schema schema, JsonParser parser) {
        this.schema = schema;
        ObjectMapper om = new ObjectMapper();
        this.nodeSupplier = () -> om.readTree(parser);
    }
    
    AutoJsonDecoder(Schema schema, InputStream in) throws IOException {
        this(schema, jsonFactory.createParser(in));
    }
    
    AutoJsonDecoder(Schema schema, String in) throws IOException {
        this(schema, jsonFactory.createParser(in));
    }
    
    AutoJsonDecoder(Schema schema, Iterable<JsonNode> nodes) {
        this.schema = schema;
        Iterator<JsonNode> iterator = nodes.iterator();
        this.nodeSupplier = () -> iterator.hasNext() ? iterator.next() : null;
    }
    
    <T> T pop() throws IOException {
        if (bin.isEmpty()) {
            JsonNode node;
            try {
                node = nodeSupplier.call();
            } catch (IOException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (node == null) {
                throw new EOFException();
            }
            List<?> list = dfs(schema, node);
            for (ListIterator<?> iter = list.listIterator(list.size()); iter.hasPrevious();) {
                bin.add(iter.previous());
            }
        }
        return (T) bin.remove(bin.size() - 1);
    }
    
    void expect(Schema schema, JsonNode node, boolean b) {
        if (!b) {
            throw new AvroTypeException("Expected " + schema.getType() + ". Got " + node.getNodeType());
        }
    }
    
    List<?> dfs(Schema schema, JsonNode node) {
        List<?> list1 = null;
        RuntimeException ex1 = null;
        try {
            list1 = dfsNonUnion(schema, node);
            if (breakAmbiguity) {
                return list1;
            }
        } catch (RuntimeException e) {
            ex1 = e;
        }
        
        List<?> list2 = null;
        RuntimeException ex2 = null;
        try {
            list2 = dfsUnion(schema, node);
        } catch (RuntimeException e) {
            ex2 = e;
        }
        
        if (ex1 != null && ex2 != null) {
            throw ex1;
        }
        
        if (list1 != null && list2 != null) {
            throw new AvroTypeException("Ambiguity: " + schema.getType() + ": " + node.getNodeType());
        }
        
        return list1 != null ? list1 : list2;
    }
    
    List<?> dfsNonUnion(Schema schema, JsonNode node) {
        switch (schema.getType()) {
            case RECORD:
                return dfsRecord(schema, node);
            case ARRAY:
                return dfsArray(schema, node);
            case MAP:
                return dfsMap(schema, node);
            case ENUM:
                return dfsEnum(schema, node);
            case FIXED:
                return dfsBytes(schema, node, schema.getFixedSize());
            case BYTES:
                return dfsBytes(schema, node, -1);
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                expect(schema, node, node.isNumber());
                return List.of(node); // parsed at read time
            case STRING:
                expect(schema, node, node.isTextual());
                return List.of(node.asText());
            case BOOLEAN:
                expect(schema, node, node.isBoolean());
                return List.of(node.asBoolean());
            case NULL:
                expect(schema, node, node.isNull());
                return List.of();
        }
        throw new IllegalArgumentException("Unknown schema type: " + schema.getType());
    }
    
    List<Object> cat(Object head, List<?> tail) {
        ArrayList<Object> list = new ArrayList<>();
        list.add(head);
        list.addAll(tail);
        return list;
    }
    
    // to prevent long->int etc
    private boolean typeMatch(String n1, String n2) {
        List<String> ns = List.of("int", "long", "float", "double");
        int i1 = ns.indexOf(n1);
        int i2 = ns.indexOf(n2);
        if (i1 != -1 && i2 != -1) {
            return i1 <= i2;
        }
        Set<String> ss = Set.of("string", "bytes");
        if (ss.contains(n1) && ss.contains(n2)) {
            return true;
        }
        return n1.equals(n2);
    }
    
    private List<?> dfsUnion(Schema schema, JsonNode node) {
        
        // {t:v}
        List<?> list1 = null;
        RuntimeException ex1 = null;
        if (node.isObject() && node.size() == 1) {
            String type = node.fieldNames().next();
            JsonNode value = node.get(type);
            
            if (schema.getType() != Schema.Type.UNION) {
                // writer union, reader not
                try {
                    if (!typeMatch(type, schema.getName())) {
                        throw new AvroTypeException("Expected " + schema.getType() + ". Got " + type);
                    }
                    list1 = dfsNonUnion(schema, value);
                } catch (RuntimeException e) {
                    // ex1 = e;
                }
            } else { // writer union, reader union
                if (schema.getIndexNamed(type) != null) { // exact name match
                    Integer index = schema.getIndexNamed(type);
                    try {
                        list1 = cat(index, dfsNonUnion(schema.getTypes().get(index), value));
                    } catch (RuntimeException e) {
                        ex1 = e;
                    }
                } else { // {"long":0} should match "float" in schema ["int", "float","double"]
                    for (int index = 0; index < schema.getTypes().size(); index++) {
                        Schema ut = schema.getTypes().get(index);
                        if (!typeMatch(type, ut.getName())) {
                            continue;
                        }
                        try {
                            list1 = cat(index, dfsNonUnion(ut, value));
                            break;
                        } catch (RuntimeException e) {
                            // ex1 = e;
                        }
                    }
                }
            }
        } // {t:v}
        if (list1 != null && breakAmbiguity) {
            return list1;
        }
        
        // any node against union: first type in union that matches
        List<?> list2 = null;
        if (schema.getType() == Schema.Type.UNION) {
            for (int index = 0; index < schema.getTypes().size(); index++) {
                try {
                    list2 = cat(index, dfsNonUnion(schema.getTypes().get(index), node));
                    break;
                } catch (RuntimeException e) {
                    continue;
                }
            }
        }
        
        if (list1 == null && list2 == null) {
            throw ex1 != null ? ex1 : new AvroTypeException("Expected " + schema.getType() + ". Got " + node.getNodeType());
        }
        
        if (list1 != null && list2 != null) {
            throw new AvroTypeException("Ambiguity: " + schema.getType() + ": " + node.getNodeType());
        }
        
        return list1 != null ? list1 : list2;
    }
    
    private List<?> dfsMap(Schema schema, JsonNode node) {
        expect(schema, node, node.isObject());
        List<Object> list = new ArrayList<>();
        int[] skip = { 0 };
        list.add(skip);
        if (!node.isEmpty()) {
            list.add((long) node.size());
            for (Map.Entry<String, JsonNode> property : node.properties()) {
                list.add(property.getKey());
                list.addAll(dfs(schema.getValueType(), property.getValue()));
            }
        }
        list.add((long) 0);
        skip[0] = list.size() - 1;
        return list;
    }
    
    private List<?> dfsArray(Schema schema, JsonNode node) {
        expect(schema, node, node.isArray());
        List<Object> list = new ArrayList<>();
        int[] skip = { 0 };
        list.add(skip);
        if (!node.isEmpty()) {
            list.add((long) node.size());
            for (JsonNode element : node) {
                list.addAll(dfs(schema.getElementType(), element));
            }
        }
        list.add((long) 0);
        skip[0] = list.size() - 1;
        return list;
    }
    
    private List<?> dfsEnum(Schema schema, JsonNode node) {
        expect(schema, node, node.isTextual());
        String text = node.asText();
        if (!schema.hasEnumSymbol(text)) {
            if (schema.getEnumDefault() == null) {
                throw new AvroTypeException("No match for " + text);
            }
            // schema resolution: reader's default is used
            text = schema.getEnumDefault();
        }
        Integer index = schema.getEnumOrdinal(text);
        return List.of(index);
    }
    
    private List<?> dfsRecord(Schema schema, JsonNode node) {
        expect(schema, node, node.isObject());
        // schema resolution:
        // - fields are matched by name/alias; ordering doesn't matter.
        // - writer's fields not in reader's schema are ignored
        // - reader's default field values are used if the field isn't in writer's.
        List<Object> list = new ArrayList<>();
        for (Schema.Field field : schema.getFields()) {
            JsonNode value = node.get(field.name());
            if (value == null) {
                for (String alias : field.aliases()) {
                    value = node.get(alias);
                    if (value != null) {
                        break;
                    }
                }
            }
            if (value == null) {
                value = Accessor.defaultValue(field);
            }
            if (value == null) {
                throw new AvroTypeException("missing required field " + field.name());
            }
            list.addAll(dfs(field.schema(), value));
        }
        return list;
    }
    
    private List<?> dfsBytes(Schema schema, JsonNode node, int optFixedSize) {
        expect(schema, node, node.isTextual());
        byte[] bytes = node.asText().getBytes(StandardCharsets.ISO_8859_1);
        if (optFixedSize != -1 && bytes.length != optFixedSize) {
            throw new AvroTypeException("Expected fixed length " + optFixedSize + ", but got" + bytes.length);
        }
        return List.of(bytes);
    }
    
    @Override
    public void readNull() throws IOException {
        // no action
    }
    
    @Override
    public boolean readBoolean() throws IOException {
        return pop();
    }
    
    @Override
    public int readInt() throws IOException {
        JsonNode node = pop();
        try {
            if (node.isIntegralNumber()) {
                return Integer.parseInt(node.asText());
            }
            if (node.isFloatingPointNumber()) { // behavior of standard JsonDecoder
                float value = Float.parseFloat(node.asText());
                if (Math.abs(value - Math.round(value)) <= Float.MIN_VALUE) {
                    return Math.round(value);
                }
            }
        } catch (NumberFormatException e) {
            //
        }
        throw new AvroTypeException("Expected " + "int" + ". Got " + node.asText());
    }
    
    @Override
    public long readLong() throws IOException {
        // schema resolution: writer's could be `int`
        JsonNode node = pop();
        try {
            if (node.isIntegralNumber()) {
                return Long.parseLong(node.asText());
            }
            if (node.isFloatingPointNumber()) { // behavior of standard JsonDecoder
                double value = Double.parseDouble(node.asText());
                if (Math.abs(value - Math.round(value)) <= Double.MIN_VALUE) {
                    return Math.round(value);
                }
            }
        } catch (NumberFormatException e) {
            //
        }
        throw new AvroTypeException("Expected " + "long" + ". Got " + node.asText());
    }
    
    @Override
    public float readFloat() throws IOException {
        // schema resolution: writer's could be `int/long`
        JsonNode node = pop();
        try {
            return Float.parseFloat(node.asText());
        } catch (NumberFormatException e) {
            throw new AvroTypeException("Expected " + "float" + ". Got " + node.asText());
        }
    }
    
    @Override
    public double readDouble() throws IOException {
        // schema resolution: writer's could be `int/long/float`
        JsonNode node = pop();
        try {
            return Double.parseDouble(node.asText());
        } catch (NumberFormatException e) {
            throw new AvroTypeException("Expected " + "double" + ". Got " + node.asText());
        }
    }
    
    @Override
    public Utf8 readString(Utf8 old) throws IOException {
        return new Utf8(readString());
    }
    
    @Override
    public String readString() throws IOException {
        // schema resolution: if writer's is `bytes`, it's encoded as a string of chars
        // in 00-FF.
        // spec is unclear on how it's promoted to `string` for reader.
        // ResolvingDecoder: bytes are interpreted as UTF-8 encoding of a string. could
        // fail.
        // This class: bytes are interpreted as ISO_8859_1 encoding of a string. won't
        // fail.
        // The reader wants to see bytes as a string, UTF-8 is definitely the dominant
        // choice.
        return pop();
    }
    
    @Override
    public void skipString() throws IOException {
        pop();
    }
    
    @Override
    public ByteBuffer readBytes(ByteBuffer old) throws IOException {
        // schema resolution: if writer's is `string`, it's encoded as a json string.
        // spec is unclear on how it's promoted to `bytes` for reader.
        // ResolvingDecoder: the string is converted to bytes in UTF-8 encoding. won't
        // fail.
        // This class: the string is converted to bytes in ISO_8859_1 encoding. could
        // fail.
        // The reader wants to see the string as bytes, UTF-8 is definitely the dominant
        // choice.
        return ByteBuffer.wrap(pop());
    }
    
    @Override
    public void skipBytes() throws IOException {
        pop();
    }
    
    byte[] popFixed(int length) throws IOException {
        byte[] bs = pop();
        if (bs.length != length) {
            throw new AvroTypeException(
                "Incorrect length for fixed binary: expected " + length + " but received " + bs.length + " bytes.");
        }
        return bs;
    }
    
    @Override
    public void readFixed(byte[] bytes, int start, int length) throws IOException {
        byte[] bs = popFixed(length);
        System.arraycopy(bs, 0, bytes, start, length);
    }
    
    @Override
    public void skipFixed(int length) throws IOException {
        popFixed(length);
    }
    
    @Override
    public int readEnum() throws IOException {
        return pop();
    }
    
    @Override
    public long readArrayStart() throws IOException {
        int[] skip = pop();
        return pop();
    }
    
    @Override
    public long arrayNext() throws IOException {
        return pop();
    }
    
    @Override
    public long skipArray() throws IOException {
        int[] skip = pop();
        for (int i = 0; i < skip[0]; i++) {
            pop();
        }
        return 0;
    }
    
    @Override
    public long readMapStart() throws IOException {
        int[] skip = pop();
        return pop();
    }
    
    @Override
    public long mapNext() throws IOException {
        return pop();
    }
    
    @Override
    public long skipMap() throws IOException {
        int[] skip = pop();
        for (int i = 0; i < skip[0]; i++) {
            pop();
        }
        return 0;
    }
    
    @Override
    public int readIndex() throws IOException {
        return pop();
    }
    
}
