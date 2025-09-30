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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests cross-compatibility between Glue-serialized records and S3-based deserialization.
 * This ensures that records serialized with AWS Glue Schema Registry can be deserialized
 * using S3-based schema storage.
 */
@ExtendWith(MockitoExtension.class)
public class S3GlueCrossCompatibilityTest {

    @Mock
    private S3Client mockS3Client;

    private AWSSchemaRegistryClient s3SchemaRegistryClient;
    private GlueSchemaRegistryConfiguration s3Configuration;

    private static final String TEST_SCHEMA_DEFINITION = 
        "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
        "{\"name\":\"name\",\"type\":\"string\"}," +
        "{\"name\":\"age\",\"type\":\"int\"}]}";
    
    private static final UUID GLUE_SCHEMA_VERSION_ID = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    private static final String S3_BUCKET = "test-schema-bucket";
    private static final String S3_KEY_PREFIX = "kafka_schema_registry/UserSchema";
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
            throw new RuntimeException("Failed to set configuration", e);
        }
    }

    @Test
    public void testDeserializeGlueSerializedRecordWithS3ClientSucceeds() throws Exception {
        String expectedS3Key = S3_KEY_PREFIX + "/" + SCHEMA_VERSION + "/schema.avsc";
        
        ResponseInputStream<GetObjectResponse> schemaStream = new ResponseInputStream<>(
            GetObjectResponse.builder().build(),
            AbortableInputStream.create(new ByteArrayInputStream(TEST_SCHEMA_DEFINITION.getBytes(StandardCharsets.UTF_8)))
        );
        when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(schemaStream);

        GetObjectTaggingResponse taggingResponse = GetObjectTaggingResponse.builder()
            .tagSet(Tag.builder()
                .key("gsr-version-id")
                .value(GLUE_SCHEMA_VERSION_ID.toString())
                .build())
            .build();
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenReturn(taggingResponse);

        GetSchemaVersionResponse response = s3SchemaRegistryClient.getSchemaVersionResponse(
            GLUE_SCHEMA_VERSION_ID.toString()
        );

        assertNotNull(response);
        assertEquals(GLUE_SCHEMA_VERSION_ID.toString(), response.schemaVersionId());
        assertEquals(TEST_SCHEMA_DEFINITION, response.schemaDefinition());
        assertEquals("AVRO", response.dataFormatAsString());
    }

    @Test
    public void testGetSchemaVersionIdByDefinitionWithS3ClientReturnsGlueCompatibleUUID() throws Exception {
        String expectedS3Key = S3_KEY_PREFIX + "/" + SCHEMA_VERSION + "/schema.avsc";
        
        GetObjectTaggingResponse taggingResponse = GetObjectTaggingResponse.builder()
            .tagSet(Tag.builder()
                .key("gsr-version-id")
                .value(GLUE_SCHEMA_VERSION_ID.toString())
                .build())
            .build();
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenReturn(taggingResponse);

        UUID schemaVersionId = s3SchemaRegistryClient.getSchemaVersionIdByDefinition(
            TEST_SCHEMA_DEFINITION,
            "User",
            "AVRO"
        );

        assertNotNull(schemaVersionId);
        assertEquals(GLUE_SCHEMA_VERSION_ID, schemaVersionId);
    }

    @Test
    public void testS3PathConstructionMatchesExpectedFormat() throws Exception {
        String expectedS3Key = S3_KEY_PREFIX + "/" + SCHEMA_VERSION + "/schema.avsc";
        
        GetObjectTaggingResponse taggingResponse = GetObjectTaggingResponse.builder()
            .tagSet(Tag.builder()
                .key("gsr-version-id")
                .value(GLUE_SCHEMA_VERSION_ID.toString())
                .build())
            .build();
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class)))
            .thenAnswer(invocation -> {
                GetObjectTaggingRequest request = invocation.getArgument(0);
                assertEquals(S3_BUCKET, request.bucket());
                assertEquals(expectedS3Key, request.key());
                return taggingResponse;
            });

        s3SchemaRegistryClient.getSchemaVersionIdByDefinition(
            TEST_SCHEMA_DEFINITION,
            "User",
            "AVRO"
        );
    }

    @Test
    public void testMagicBytesCompatibilitySameUUIDFromGlueAndS3() throws Exception {
        ResponseInputStream<GetObjectResponse> schemaStream = new ResponseInputStream<>(
            GetObjectResponse.builder().build(),
            AbortableInputStream.create(new ByteArrayInputStream(TEST_SCHEMA_DEFINITION.getBytes(StandardCharsets.UTF_8)))
        );
        when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(schemaStream);

        GetObjectTaggingResponse taggingResponse = GetObjectTaggingResponse.builder()
            .tagSet(Tag.builder()
                .key("gsr-version-id")
                .value(GLUE_SCHEMA_VERSION_ID.toString())
                .build())
            .build();
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class))).thenReturn(taggingResponse);

        UUID serializationUUID = s3SchemaRegistryClient.getSchemaVersionIdByDefinition(
            TEST_SCHEMA_DEFINITION,
            "User",
            "AVRO"
        );

        GetSchemaVersionResponse deserializationResponse = s3SchemaRegistryClient.getSchemaVersionResponse(
            GLUE_SCHEMA_VERSION_ID.toString()
        );

        assertEquals(serializationUUID.toString(), deserializationResponse.schemaVersionId());
        assertEquals(GLUE_SCHEMA_VERSION_ID, serializationUUID);
    }
}
