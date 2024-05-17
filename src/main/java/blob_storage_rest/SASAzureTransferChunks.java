package blob_storage_rest;

import com.azure.storage.blob.*;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.blob.sas.*;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class SASAzureTransferChunks {
    private static final String SOURCE_STORAGE_CONNECTION_STRING = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
    private static final String DEST_STORAGE_CONNECTION_STRING = System.getenv("AZURE_STORAGE_CONNECTION_STRING_TARGET");

    public static void main(String[] args) throws Exception {
        String storageAccountNameSource = getAccountNameFromConnectionString(SOURCE_STORAGE_CONNECTION_STRING);
        String storageAccountKeySource = getAccountKeyFromConnectionString(SOURCE_STORAGE_CONNECTION_STRING);
        String storageAccountNameTarget = getAccountNameFromConnectionString(DEST_STORAGE_CONNECTION_STRING);
        String storageAccountKeyTarget = getAccountKeyFromConnectionString(DEST_STORAGE_CONNECTION_STRING);
        String sourceContainerName = "large-blob";
        String destinationContainerName = "target-container";
        String sourceBlobName = "UniversityTimeTable.jpeg";
        String destinationBlobName = "UniversityTimeTable.jpeg";

        // Create StorageSharedKeyCredential
        StorageSharedKeyCredential credentialSource = new StorageSharedKeyCredential(storageAccountNameSource, storageAccountKeySource);
        StorageSharedKeyCredential credentialTarget = new StorageSharedKeyCredential(storageAccountNameTarget, storageAccountKeyTarget);

        // Create BlobServiceClient
        BlobServiceClient blobServiceClientSource = new BlobServiceClientBuilder()
            .endpoint(String.format("https://%s.blob.core.windows.net", storageAccountNameSource))
            .credential(credentialSource)
            .buildClient();

        BlobServiceClient blobServiceClientTarget = new BlobServiceClientBuilder()
            .endpoint(String.format("https://%s.blob.core.windows.net", storageAccountNameTarget))
            .credential(credentialTarget)
            .buildClient();

        // Generate SAS token for the source container
        String sourceSasToken = generateSasToken(blobServiceClientSource, sourceContainerName, BlobSasPermission.parse("r"));

        // Generate SAS token for the destination container
        String destinationSasToken = generateSasToken(blobServiceClientTarget, destinationContainerName, BlobSasPermission.parse("w"));

        // Source and destination URLs
        String sourceBlobUrl = String.format(
            "https://%s.blob.core.windows.net/%s/%s?%s",
            storageAccountNameSource, sourceContainerName, sourceBlobName, sourceSasToken);

        String destinationBlobUrl = String.format(
            "https://%s.blob.core.windows.net/%s/%s?%s",
            storageAccountNameTarget, destinationContainerName, destinationBlobName, destinationSasToken);

        // Perform GET and PUT operations in chunks
        InputStream blobInputStream = getBlob(sourceBlobUrl);
        transferBlobInChunks(destinationBlobUrl, blobInputStream);
        System.out.println("Blob transfer completed successfully.");
    }

    private static String generateSasToken(BlobServiceClient blobServiceClient, String containerName, BlobSasPermission permissions) {
        // Get container client
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);

        // Define SAS token expiration time
        OffsetDateTime expiryTime = OffsetDateTime.now(ZoneOffset.UTC).plusHours(1);

        // Generate SAS token
        BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(expiryTime, permissions)
            .setStartTime(OffsetDateTime.now(ZoneOffset.UTC));
        
        return containerClient.generateSas(values);
    }

    private static InputStream getBlob(String blobUrl) throws Exception {
        URI uri = new URI(blobUrl);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");

        int responseCode = connection.getResponseCode();
        if (responseCode == 200) {
            return connection.getInputStream();
        } else {
            throw new RuntimeException("Failed to download blob: HTTP " + responseCode);
        }
    }

    private static void transferBlobInChunks(String blobUrl, InputStream blobInputStream) throws Exception {
        int chunkSize = 4 * 1024 * 1024; // 4MB chunk size
        byte[] buffer = new byte[chunkSize];
        int bytesRead;
        int blockIdCount = 0;
        List<String> blockIds = new ArrayList<>();

        while ((bytesRead = blobInputStream.read(buffer)) != -1) {
            String blockId = Base64.getEncoder().encodeToString(String.format("%05d", blockIdCount).getBytes());
            blockIds.add(blockId);
            uploadBlock(blobUrl, buffer, bytesRead, blockId);
            blockIdCount++;
        }

        commitBlocks(blobUrl, blockIds);
    }

    private static void uploadBlock(String blobUrl, byte[] data, int dataLength, String blockId) throws Exception {
        String blockUrl = blobUrl + "&comp=block&blockid=" + blockId;
        URI uri = new URI(blockUrl);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);

        try (OutputStream outStream = connection.getOutputStream()) {
            outStream.write(data, 0, dataLength);
        }

        int responseCode = connection.getResponseCode();
        if (responseCode != 201) {
            throw new RuntimeException("Failed to upload block: HTTP " + responseCode);
        }
    }

    private static void commitBlocks(String blobUrl, List<String> blockIds) throws Exception {
        String blockListXml = buildBlockListXml(blockIds);
        String commitUrl = blobUrl + "&comp=blocklist";
        URI uri = new URI(commitUrl);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/xml");

        try (OutputStream outStream = connection.getOutputStream()) {
            outStream.write(blockListXml.getBytes());
        }

        int responseCode = connection.getResponseCode();
        if (responseCode != 201) {
            throw new RuntimeException("Failed to commit blocks: HTTP " + responseCode);
        }
    }

    private static String buildBlockListXml(List<String> blockIds) {
        StringBuilder xmlBuilder = new StringBuilder();
        xmlBuilder.append("<?xml version=\"1.0\" encoding=\"utf-8\"?>");
        xmlBuilder.append("<BlockList>");
        for (String blockId : blockIds) {
            xmlBuilder.append("<Latest>").append(blockId).append("</Latest>");
        }
        xmlBuilder.append("</BlockList>");
        return xmlBuilder.toString();
    }

    private static String getAccountNameFromConnectionString(String connectionString) {
        String[] parts = connectionString.split(";");
        for (String part : parts) {
            if (part.startsWith("AccountName=")) {
                return part.split("=")[1];
            }
        }
        throw new IllegalArgumentException("Invalid connection string");
    }

    private static String getAccountKeyFromConnectionString(String connectionString) {
        String[] parts = connectionString.split(";");
        for (String part : parts) {
            if (part.startsWith("AccountKey=")) {
                String key = part.split("=")[1];
                // Check if the key ends with '=='
                if (!key.endsWith("==")) {
                    key += "==";
                }
                return key;
            }
        }
        throw new IllegalArgumentException("Invalid connection string");
    }
}

