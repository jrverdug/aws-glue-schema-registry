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
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ApiName;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.urlconnection.ProxyConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.Tag;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;

/**
 * Handles all the requests related to the schema management.
 */
@Slf4j
public class AWSSchemaRegistryClient {

    private static final int MAX_ATTEMPTS = 10;
    private static final long MAX_WAIT_INTERVAL = 3000;

    private final S3Client s3Client;
    private GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration;

    /**
     * Create Amazon Schema Registry Client.
     *
     * @param credentialsProvider           credentials provider
     * @param glueSchemaRegistryConfiguration schema registry configuration elements
     * @throws AWSSchemaRegistryException on any error while building the client
     */
    public AWSSchemaRegistryClient(@NonNull AwsCredentialsProvider credentialsProvider,
                                   @NonNull GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration,
                                   @NonNull RetryPolicy retryPolicy) {
        this.glueSchemaRegistryConfiguration = glueSchemaRegistryConfiguration;
        ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .addExecutionInterceptor(new UserAgentRequestInterceptor())
                .build();
        UrlConnectionHttpClient.Builder urlConnectionHttpClientBuilder = UrlConnectionHttpClient.builder();
        if (glueSchemaRegistryConfiguration.getProxyUrl() != null) {
        	log.debug("Creating http client using proxy {}", glueSchemaRegistryConfiguration.getProxyUrl().toString());
    		ProxyConfiguration proxy = ProxyConfiguration.builder().endpoint(glueSchemaRegistryConfiguration.getProxyUrl()).build();
    		urlConnectionHttpClientBuilder.proxyConfiguration(proxy);
        }

        S3ClientBuilder s3ClientBuilder = S3Client
                .builder()
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(overrideConfiguration)
                .httpClient(urlConnectionHttpClientBuilder.build())
                .region(Region.of(glueSchemaRegistryConfiguration.getRegion()));

        if (glueSchemaRegistryConfiguration.getEndPoint() != null) {
            try {
                s3ClientBuilder.endpointOverride(new URI(glueSchemaRegistryConfiguration.getEndPoint()));
            } catch (URISyntaxException e) {
                String message = String.format("Malformed uri, please pass the valid uri for creating the client",
                                               glueSchemaRegistryConfiguration.getEndPoint());
                throw new AWSSchemaRegistryException(message, e);
            }
        }
        this.s3Client = s3ClientBuilder.build();
    }

    /**
     * Create Amazon Schema Registry Client.
     *
     * @param credentialsProvider           credentials provider
     * @param glueSchemaRegistryConfiguration schema registry configuration elements
     * @throws AWSSchemaRegistryException on any error while building the client
     */
    public AWSSchemaRegistryClient(@NonNull AwsCredentialsProvider credentialsProvider,
                                   @NonNull GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration) {
        this(credentialsProvider, glueSchemaRegistryConfiguration, RetryPolicy.defaultRetryPolicy());
    }

    public AWSSchemaRegistryClient(@NonNull S3Client s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Get Schema Version ID by passing the schema definition.
     * 
     * @param schemaDefinition Schema Definition - IGNORED in S3 mode
     * @param schemaName       Schema Name - IGNORED in S3 mode  
     * @param dataFormat       Data Format - IGNORED in S3 mode
     * @return                 Schema Version ID from S3 gsr-version-id tag
     * @throws AWSSchemaRegistryException S3 mode doesn't support schema lookup by definition.
     *                                   Use getSchemaVersionResponse() to fetch from S3.
     */
    public UUID getSchemaVersionIdByDefinition(@NonNull String schemaDefinition, @NonNull String schemaName,
                                               @NonNull String dataFormat) throws AWSSchemaRegistryException {
        throw new AWSSchemaRegistryException(
            "getSchemaVersionIdByDefinition is not supported in S3 mode. " +
            "S3 mode is read-only and fetches schema from a fixed location. " +
            "Use getSchemaVersionResponse() to fetch schema from S3 instead."
        );
    }

    /**
     * Get the schema definition by fetching schema.avsc from S3 and reading gsr-version-id tag.
     *
     * @param schemaVersionId IGNORED in S3 mode - kept for backwards compatibility with API contract.
     *                        The actual UUID is read from the gsr-version-id tag on the S3 object.
     * @return                schema definition from S3 with UUID from gsr-version-id tag
     * @throws AWSSchemaRegistryException on any errors during schema retrieval from S3
     */
    public GetSchemaVersionResponse getSchemaVersionResponse(@NonNull String schemaVersionId)
            throws AWSSchemaRegistryException {
        try {
            String s3Key = buildS3SchemaKey();
            
            log.debug("Fetching schema from S3: s3://{}/{}", 
                    glueSchemaRegistryConfiguration.getS3BucketName(), s3Key);
            
            // Fetch schema.avsc content from S3
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(glueSchemaRegistryConfiguration.getS3BucketName())
                    .key(s3Key)
                    .build();

            byte[] schemaBytes = s3Client.getObject(getObjectRequest).readAllBytes();
            String schemaDefinition = new String(schemaBytes, StandardCharsets.UTF_8);

            // Fetch gsr-version-id from S3 object tags
            GetObjectTaggingRequest taggingRequest = GetObjectTaggingRequest.builder()
                    .bucket(glueSchemaRegistryConfiguration.getS3BucketName())
                    .key(s3Key)
                    .build();
            
            GetObjectTaggingResponse taggingResponse = s3Client.getObjectTagging(taggingRequest);
            String gsrVersionId = getTagValue(taggingResponse, "gsr-version-id")
                    .orElseThrow(() -> new AWSSchemaRegistryException("gsr-version-id tag not found on S3 object"));

            // Build response compatible with Glue format using the UUID from the tag
            return GetSchemaVersionResponse.builder()
                    .schemaVersionId(gsrVersionId)
                    .schemaDefinition(schemaDefinition)
                    .dataFormat("AVRO")
                    .build();

        } catch (NoSuchKeyException e) {
            String errorMessage = String.format("Schema file not found in S3: s3://%s/%s", 
                    glueSchemaRegistryConfiguration.getS3BucketName(), buildS3SchemaKey());
            throw new AWSSchemaRegistryException(errorMessage, e);
        } catch (Exception e) {
            String errorMessage = String.format("Failed to get schema from S3: %s", e.getMessage());
            throw new AWSSchemaRegistryException(errorMessage, e);
        }
    }

    private GetSchemaVersionRequest getSchemaVersionRequest(String schemaVersionId) {
        GetSchemaVersionRequest getSchemaVersionRequest = GetSchemaVersionRequest.builder()
                .schemaVersionId(schemaVersionId).build();
        return getSchemaVersionRequest;
    }

    private void validateSchemaVersionResponse(GetSchemaVersionResponse schemaVersionResponse, String schemaVersionId) {
        if (schemaVersionResponse == null || schemaVersionResponse.schemaVersionId() == null) {
            String message = String.format("Schema definition is not present for the schema id = %s", schemaVersionId);
            throw new AWSSchemaRegistryException(message);
        }
    }

    private UUID returnSchemaVersionIdIfAvailable(GetSchemaByDefinitionResponse response) {
        if (response.schemaVersionId() != null
                && response.statusAsString().equals(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())) {
            return UUID.fromString(response.schemaVersionId());
        } else {
            String msg = String.format("Schema Found but status is %s", response.statusAsString());
            throw new AWSSchemaRegistryException(msg);
        }
    }

    /**
     * Create a request to get a schema using the schema definition and the schema name.
     *
     * @param schemaDefinition Schema Definition
     * @param schemaName       Schema Name
     * @return                 GetSchemaByDefinitionRequest object
     */
    public GetSchemaByDefinitionRequest buildGetSchemaByDefinitionRequest(String schemaDefinition, String schemaName) {
        return buildGetSchemaByDefinitionRequest(schemaDefinition, schemaName, glueSchemaRegistryConfiguration.getRegistryName());
    }

    /**
     * Create a request to get a schema using the schema definition and the schema name.
     *
     * @param schemaDefinition Schema Definition
     * @param schemaName       Schema Name
     * @param registryName     Registry name
     * @return                 GetSchemaByDefinitionRequest object
     */
    public GetSchemaByDefinitionRequest buildGetSchemaByDefinitionRequest(String schemaDefinition, String schemaName,
                                                                          String registryName) {
        GetSchemaByDefinitionRequest request = GetSchemaByDefinitionRequest.builder()
                .schemaId(getSchemaIdRequestObject(schemaName, registryName))
                .schemaDefinition(schemaDefinition).build();
        return request;
    }

    /**
     * Create a schema - NOT SUPPORTED in S3 mode.
     * @param schemaName Schema Name - IGNORED
     * @param dataFormat Data Format - IGNORED  
     * @param schemaDefinition Schema Definition - IGNORED
     * @param metadata schema version metadata - IGNORED
     * @return           Never returns - always throws exception
     * @throws AWSSchemaRegistryException S3 mode is read-only, schema creation not supported
     */
    public UUID createSchema(String schemaName,
                             String dataFormat,
                             String schemaDefinition,
                             Map<String, String> metadata) throws AWSSchemaRegistryException {
        throw new AWSSchemaRegistryException(
            "createSchema is not supported in S3 mode. " +
            "S3 mode is read-only. Schemas must be pre-uploaded to S3 via external process."
        );
    }

    /**
     * Register the schema - NOT SUPPORTED in S3 mode.
     * @param schemaDefinition Schema Definition - IGNORED
     * @param schemaName       Schema Name - IGNORED
     * @param dataFormat       Data Format - IGNORED
     * @param metadata         Metadata Map - IGNORED
     * @return                 Never returns - always throws exception
     * @throws AWSSchemaRegistryException S3 mode is read-only, schema registration not supported
     */
    public UUID registerSchemaVersion(String schemaDefinition, String schemaName, String dataFormat, Map<String, String> metadata) {
        throw new AWSSchemaRegistryException(
            "registerSchemaVersion is not supported in S3 mode. " +
            "S3 mode is read-only. Schemas must be pre-uploaded to S3 via external process."
        );
    }

    /**
     * Register the schema - NOT SUPPORTED in S3 mode.
     * @param schemaDefinition Schema Definition - IGNORED
     * @param schemaName       Schema Name - IGNORED
     * @param dataFormat       Data Format - IGNORED
     * @return                 Never returns - always throws exception
     * @throws AWSSchemaRegistryException S3 mode is read-only, schema registration not supported
     */
    public GetSchemaVersionResponse registerSchemaVersion(String schemaDefinition, String schemaName, String dataFormat) throws AWSSchemaRegistryException {
        throw new AWSSchemaRegistryException(
            "registerSchemaVersion is not supported in S3 mode. " +
            "S3 mode is read-only. Schemas must be pre-uploaded to S3 via external process."
        );
    }

    private GetSchemaVersionResponse transformToGetSchemaVersionResponse(RegisterSchemaVersionResponse registerSchemaVersionResponse) {
        return GetSchemaVersionResponse.builder()
                .schemaVersionId(registerSchemaVersionResponse.schemaVersionId())
                .status(registerSchemaVersionResponse.status())
                .status(registerSchemaVersionResponse.statusAsString())
                .versionNumber(registerSchemaVersionResponse.versionNumber())
                .build();
    }

    private CreateSchemaRequest getCreateSchemaRequestObject(String schemaName, String dataFormat, String schemaDefinition) {
        return CreateSchemaRequest
                .builder()
                .dataFormat(DataFormat.valueOf(dataFormat))
                .description(glueSchemaRegistryConfiguration.getDescription())
                .registryId(RegistryId.builder().registryName(glueSchemaRegistryConfiguration.getRegistryName()).build())
                .schemaName(schemaName)
                .schemaDefinition(schemaDefinition)
                .compatibility(glueSchemaRegistryConfiguration.getCompatibilitySetting())
                .tags(glueSchemaRegistryConfiguration.getTags())
                .build();
    }

    private RegisterSchemaVersionRequest getRegisterSchemaVersionRequest(String schemaDefinition, String schemaName) {
        return RegisterSchemaVersionRequest
                .builder()
                .schemaDefinition(schemaDefinition)
                .schemaId(getSchemaIdRequestObject(schemaName, glueSchemaRegistryConfiguration.getRegistryName()))
                .build();
    }

    private SchemaId getSchemaIdRequestObject(@NonNull String schemaName, @NonNull String registryName) {
        return SchemaId
                .builder()
                .schemaName(schemaName)
                .registryName(registryName)
                .build();
    }

    private GetSchemaVersionRequest getGetSchemaVersionRequest(String schemaVersionId) {
        return GetSchemaVersionRequest
                .builder()
                .schemaVersionId(schemaVersionId)
                .build();
    }

    /**
     * Get schema version response of asynchronous operation.
     *
     * @return Schema version.
     */
    private GetSchemaVersionResponse waitForSchemaEvolutionCheckToComplete(GetSchemaVersionRequest getSchemaVersionRequest) {

        GetSchemaVersionResponse response;

        try {
            int retries = 0;

            Thread.sleep(MAX_WAIT_INTERVAL);

            do {
                response = client.getSchemaVersion(getSchemaVersionRequest);

                if (AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString()
                        .equals(response.statusAsString())) {
                    return response;
                } else if (!AWSSchemaRegistryConstants.SchemaVersionStatus.PENDING.toString()
                        .equals(response.statusAsString())) {
                    throw new AWSSchemaRegistryException(String.format("Schema evolution check failed. "
                                                                       + "schemaVersionId %s is in %s status.",
                                                                       getSchemaVersionRequest.schemaVersionId(),
                                                                       response.statusAsString()));
                }

            } while (retries++ < MAX_ATTEMPTS - 1);

            if (retries >= MAX_ATTEMPTS && !AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString()
                    .equals(response.statusAsString())) {
                throw new AWSSchemaRegistryException(String.format("Retries exhausted for schema evolution check for "
                                                                   + "schemaVersionId = %s",
                                                                   getSchemaVersionRequest.schemaVersionId()));
            }
        } catch (Exception ex) {
            String message =
                    String.format("Exception occurred, while performing schema evolution check for schemaVersionId = "
                                  + "%s", getSchemaVersionRequest.schemaVersionId());
            throw new AWSSchemaRegistryException(message, ex);
        }
        return response;
    }

    /**
     * Put metadata to schema version - NOT SUPPORTED in S3 mode.
     * @param schemaVersionId Schema Version Id - IGNORED
     * @param metadata Metadata Map - IGNORED
     * @throws AWSSchemaRegistryException S3 mode is read-only, metadata operations not supported
     */
    public void putSchemaVersionMetadata(UUID schemaVersionId, Map<String, String> metadata) {
        throw new AWSSchemaRegistryException(
            "putSchemaVersionMetadata is not supported in S3 mode. " +
            "S3 mode is read-only. Use S3 object tags for metadata instead."
        );
    }

    /**
     * Put metadata to schema version - NOT SUPPORTED in S3 mode.
     * @param schemaVersionId Schema Version Id - IGNORED
     * @param metadataKeyValuePair Metadata Key Value Pair - IGNORED
     * @return           Never returns - always throws exception
     * @throws AWSSchemaRegistryException S3 mode is read-only, metadata operations not supported
     */
    public Object putSchemaVersionMetadata(UUID schemaVersionId, MetadataKeyValuePair metadataKeyValuePair)
            throws AWSSchemaRegistryException {
        throw new AWSSchemaRegistryException(
            "putSchemaVersionMetadata is not supported in S3 mode. " +
            "S3 mode is read-only. Use S3 object tags for metadata instead."
        );
    }

    private PutSchemaVersionMetadataRequest createPutSchemaVersionMetadataRequest(UUID schemaVersionId, MetadataKeyValuePair metadataKeyValuePair) {
        return PutSchemaVersionMetadataRequest
                .builder()
                .schemaVersionId(schemaVersionId.toString())
                .metadataKeyValue(metadataKeyValuePair)
                .build();
    }

    private MetadataKeyValuePair createMetadataKeyValuePair(Map.Entry<String, String> metadataEntry) {
        return MetadataKeyValuePair
                .builder()
                .metadataKey(metadataEntry.getKey())
                .metadataValue(metadataEntry.getValue())
                .build();
    }

    /**
     * Query metadata for schema version - NOT SUPPORTED in S3 mode.
     *
     * @param schemaVersionId Schema Version Id - IGNORED
     * @return Never returns - always throws exception
     * @throws AWSSchemaRegistryException S3 mode doesn't support metadata queries, use S3 object tags
     */
    public Object querySchemaVersionMetadata(UUID schemaVersionId) {
        throw new AWSSchemaRegistryException(
            "querySchemaVersionMetadata is not supported in S3 mode. " +
            "Use S3 GetObjectTagging API to read object tags instead."
        );
    }

    private QuerySchemaVersionMetadataRequest createQuerySchemaVersionMetadataRequest(UUID schemaVersionId) {
        return QuerySchemaVersionMetadataRequest
                .builder()
                .schemaVersionId(schemaVersionId.toString())
                .build();
    }

    /**
     * Query Schema Tags - NOT SUPPORTED in S3 mode.
     * @param schemaDefinition  Schema Definition - IGNORED
     * @param schemaName        Schema Name - IGNORED
     * @return Never returns - always throws exception
     * @throws AWSSchemaRegistryException S3 mode doesn't support schema lookup, use S3 object tags
     */
    public Object querySchemaTags(String schemaDefinition, String schemaName) {
        throw new AWSSchemaRegistryException(
            "querySchemaTags is not supported in S3 mode. " +
            "Use S3 GetObjectTagging API to read schema object tags instead."
        );
    }

    /**
     * AWS SDK Request interceptor that adds additional data to the UserAgent of Glue API requests.
     */
    @VisibleForTesting
    protected class UserAgentRequestInterceptor implements ExecutionInterceptor {
        private static final String ONE = "1";
        private static final String ZERO = "0";

        @Override
        public SdkRequest modifyRequest(Context.ModifyRequest context, ExecutionAttributes executionAttributes) {
            if (!(context.request() instanceof GlueRequest)) {
                //Only applies to Glue requests.
                return context.request();
            }

            GlueRequest request = (GlueRequest) context.request();
            AwsRequestOverrideConfiguration overrideConfiguration =
                request.overrideConfiguration().map(config ->
                    config
                        .toBuilder()
                        .addApiName(getApiName())
                        .build())
                    .orElse((AwsRequestOverrideConfiguration.builder()
                        .addApiName(getApiName())
                        .build()));

            return request.toBuilder().overrideConfiguration(overrideConfiguration).build();
        }

        private ApiName getApiName() {
            return ApiName.builder()
                .version(com.amazonaws.services.schemaregistry.common.MavenPackaging.VERSION)
                .name(buildUserAgentSuffix())
                .build();
        }

        private String buildUserAgentSuffix() {
            Map<String, String> userAgentSuffixItems = ImmutableMap.of(
                "autoreg", glueSchemaRegistryConfiguration.isSchemaAutoRegistrationEnabled() ? ONE : ZERO,
                "compress", glueSchemaRegistryConfiguration.getCompressionType().equals(
                    AWSSchemaRegistryConstants.COMPRESSION.ZLIB) ? ONE : ZERO,
                "secdeser", glueSchemaRegistryConfiguration.getSecondaryDeserializer() != null ? ONE : ZERO,
                "app", glueSchemaRegistryConfiguration.getUserAgentApp()
            );

            StringJoiner userAgentSuffix = new StringJoiner(":");

            userAgentSuffixItems
                .forEach((key, value) -> userAgentSuffix.add(key + "/" + value));

            return userAgentSuffix.toString();
        }
    }

    /**
     * Build S3 key for schema.avsc file using configured prefix
     */
    private String buildS3SchemaKey() {
        String prefix = glueSchemaRegistryConfiguration.getS3KeyPrefix();
        if (prefix == null || prefix.isEmpty()) {
            return "schema.avsc";
        }
        // Ensure proper path separator
        return prefix.endsWith("/") ? prefix + "schema.avsc" : prefix + "/schema.avsc";
    }

    /**
     * Extract tag value from S3 object tags
     */
    private Optional<String> getTagValue(GetObjectTaggingResponse taggingResponse, String tagKey) {
        return taggingResponse.tagSet().stream()
                .filter(tag -> tagKey.equals(tag.key()))
                .map(Tag::value)
                .findFirst();
    }
}