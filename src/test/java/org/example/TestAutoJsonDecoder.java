package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestAutoJsonDecoder {
    
    String toJson(GenericRecord record, Schema schema) throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, os);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        datumWriter.write(record, encoder);
        encoder.flush();
        return os.toString(StandardCharsets.UTF_8);
    }
    
    String jsonEncoded() throws Exception {
        Schema writersSchema = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "Thing",
              "fields": [
                { "name": "size",  "type": "int" },
                { "name": "heavy", "type": "boolean" },
                { "name": "desc",  "type": ["null", "string"] }
              ]
            }""");

        GenericRecord record = new GenericData.Record(writersSchema);
        record.put("size", 123);
        record.put("heavy", false);
        record.put("desc", "parsley");
        return toJson(record, writersSchema);
    }
    
    @Test
    void jsonDecode() throws Exception {
        String json = jsonEncoded();
        assertEquals(json, """
            {"size":123,"heavy":false,"desc":{"string":"parsley"}}""");
        
        Schema readersSchema = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "Thing",
              "fields": [
                { "name": "size", "type": "float" },
                { "name": "note", "type": "string", "aliases": ["desc"] },
                { "name": "good", "type": "boolean", "default": true }
              ]
            }""");
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(readersSchema, readersSchema);
        AutoJsonDecoder decoder = new AutoJsonDecoder(readersSchema, json);
        GenericRecord record = datumReader.read(null, decoder);
        
        String json2 = toJson(record, readersSchema);
        assertEquals(json2, """
            {"size":123.0,"note":"parsley","good":true}""");
    }
}