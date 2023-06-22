package com.example.schema.registry;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;

public class CustomAvroByteReader<T extends SpecificRecordBase> {
    final SpecificDatumReader<T> reader;

    public CustomAvroByteReader(final Schema schema) {
        this.reader = new SpecificDatumReader<>(schema);
    }

    public T deserialize(byte[] bytes) {
        // Omitir primeros 5 bytes
        // Byte 0: Magic Byte -> Siempre es 0 si el mensje se serializo con Confluent Platform
        // Bytes 1-4: Schema ID -> ID de esquema de 4 bytes tal como lo devuelve Schema Registry.
        // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
        final byte[] bytesWithoutHeader = new byte[bytes.length - 5];
        System.arraycopy(bytes, 5, bytesWithoutHeader, 0, bytes.length - 5);

        Decoder decoder = DecoderFactory.get().binaryDecoder(bytesWithoutHeader, null);
        try {
            return reader.read(null, decoder);
        } catch (IOException e) {
            return null;
        }
    }
}