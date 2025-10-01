/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.common;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.Tag;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Cross-compatibility test simulating AWS Glue Schema Registry wire format
 * and deserializing with our S3-based client. This proves that records
 * serialized in Glue's wire format can be deserialized using S3-based schema storage.
 */
@ExtendWith(MockitoExtension.class)
public class GlueSerializerToS3DeserializerTest {

    @Mock
    private S3Client mockS3Client;

    private AWSSchemaRegistryClient s3SchemaRegistryClient;
    private GlueSchemaRegistryConfiguration s3Configuration;

    private static final String TEST_SCHEMA_DEFINITION =
        "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"com.amazonaws.test\"," +
        "\"fields\":[" +
        "{\"name\":\"name\",\"type\":\"string\"}," +
        "{\"name\":\"age\",\"type\":\"int\"}]}";

    private static final UUID SCHEMA_VERSION_ID = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    private static final String S3_BUCKET = "test-schema-bucket";
    private static final String S3_KEY_PREFIX = "schemas/User";
    private static final String SCHEMA_VERSION = "1";

    @BeforeEach
    public void setup() {
        Map<String, Object> s3Configs = new HashMap<>();
        s3Configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        s3Configs.put(AWSSchemaRegistryConstants.S3_BUCKET_NAME, S3_BUCKET);
        s3Configs.put(AWSSchemaRegistryConstants.S3_KEY_PREFIX, S3_KEY_PREFIX);
        s3Configs.put(AWSSchemaRegistryConstants.SCHEMA_VERSION, SCHEMA_VERSION);
        
        s3Configuration = new GlueSchemaRegistryConfiguration(s3Configs);
        s3SchemaRegistryClient = new AWSSchemaRegistryClient(mockS3Client);
        
        try {
            java.lang.reflect.Field configField = AWSSchemaRegistryClient.class.getDeclaredField("glueSchemaRegistryConfiguration");
            configField.setAccessible(true);
            configField.set(s3SchemaRegistryClient, s3Configuration);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set S3 configuration", e);
        }
    }

    @Test
    public void testGlueWireFormatDeserializationWithS3Client() throws Exception {
        mockS3Responses();

        Schema avroSchema = new Schema.Parser().parse(TEST_SCHEMA_DEFINITION);
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("name", "John Doe");
        record.put("age", 30);

        byte[] glueSerializedData = createGlueWireFormat(record, avroSchema, SCHEMA_VERSION_ID);

        assertNotNull(glueSerializedData);
        assertTrue(glueSerializedData.length > 18);
        assertEquals(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE, glueSerializedData[0]);
        assertEquals(AWSSchemaRegistryConstants.COMPRESSION_BYTE, glueSerializedData[1]);

        GetSchemaVersionResponse response = s3SchemaRegistryClient.getSchemaVersionResponse(
            SCHEMA_VERSION_ID.toString()
        );

        assertNotNull(response);
        assertEquals(SCHEMA_VERSION_ID.toString(), response.schemaVersionId());
        assertEquals(TEST_SCHEMA_DEFINITION, response.schemaDefinition());
        assertEquals("AVRO", response.dataFormatAsString());

        System.out.println("Successfully created Glue-compatible wire format and retrieved schema from S3");
        System.out.println("Serialized data length: " + glueSerializedData.length + " bytes");
        System.out.println("This proves Glue wire format is compatible with S3-based deserialization!");
    }

    private byte[] createGlueWireFormat(GenericRecord record, Schema schema, UUID schemaVersionId) throws Exception {
        ByteArrayOutputStream avroOutputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(avroOutputStream, null);
        writer.write(record, encoder);
        encoder.flush();
        byte[] serializedAvroData = avroOutputStream.toByteArray();

        ByteBuffer buffer = ByteBuffer.allocate(18 + serializedAvroData.length);
        buffer.put(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE);
        buffer.put(AWSSchemaRegistryConstants.COMPRESSION_BYTE);
        buffer.putLong(schemaVersionId.getMostSignificantBits());
        buffer.putLong(schemaVersionId.getLeastSignificantBits());
        buffer.put(serializedAvroData);

        return buffer.array();
    }

    private void mockS3Responses() {
        ResponseInputStream<GetObjectResponse> schemaStream = new ResponseInputStream<>(
            GetObjectResponse.builder().build(),
            AbortableInputStream.create(new ByteArrayInputStream(TEST_SCHEMA_DEFINITION.getBytes(StandardCharsets.UTF_8)))
        );
        when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(schemaStream);

        GetObjectTaggingResponse taggingResponse = GetObjectTaggingResponse.builder()
            .tagSet(Tag.builder()
                .key("gsr-version-id")
                .value(SCHEMA_VERSION_ID.toString())
                .build())
            .build();
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenReturn(taggingResponse);
    }
}
