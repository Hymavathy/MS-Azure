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

public class SASTokenAzureTransfer {
    private static final String SOURCE_STORAGE_CONNECTION_STRING = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
    private static final String DEST_STORAGE_CONNECTION_STRING = System
    .getenv("AZURE_STORAGE_CONNECTION_STRING_TARGET");
    public static void main(String[] args) throws Exception {

        String storageAccountNameSource = getAccountNameFromConnectionString(SOURCE_STORAGE_CONNECTION_STRING);
        String storageAccountKeySource = getAccountKeyFromConnectionString(SOURCE_STORAGE_CONNECTION_STRING);
        String storageAccountNameTarget = getAccountNameFromConnectionString(DEST_STORAGE_CONNECTION_STRING);
        String storageAccountKeyTarget = getAccountKeyFromConnectionString(DEST_STORAGE_CONNECTION_STRING);
        String sourceContainerName = "large-blob";
        String destinationContainerName = "target-container";
        String sourceBlobName =  "UniversityTimeTable.jpeg";
        String destinationBlobName =  "UniversityTimeTable.jpeg";

        // Create StorageSharedKeyCredential
        StorageSharedKeyCredential credential_source = new StorageSharedKeyCredential(storageAccountNameSource, storageAccountKeySource);
        StorageSharedKeyCredential credential_target = new StorageSharedKeyCredential(storageAccountNameTarget, storageAccountKeyTarget);

        // Create BlobServiceClient
        BlobServiceClient blobServiceClient1 = new BlobServiceClientBuilder()
            .endpoint(String.format("https://%s.blob.core.windows.net", storageAccountNameSource))
            .credential(credential_source)
            .buildClient();

        BlobServiceClient blobServiceClient2 = new BlobServiceClientBuilder()
            .endpoint(String.format("https://%s.blob.core.windows.net", storageAccountNameTarget))
            .credential(credential_target)
            .buildClient();

        // Generate SAS token for the source container
        String sourceSasToken = generateSasToken(blobServiceClient1, sourceContainerName, BlobSasPermission.parse("r"));

        // Generate SAS token for the destination container
        String destinationSasToken = generateSasToken(blobServiceClient2, destinationContainerName, BlobSasPermission.parse("w"));

        // Source and destination URLs
        String sourceBlobUrl = String.format(
            "https://%s.blob.core.windows.net/%s/%s?%s",
            storageAccountNameSource, sourceContainerName, sourceBlobName, sourceSasToken);

        String destinationBlobUrl = String.format(
            "https://%s.blob.core.windows.net/%s/%s?%s",
            storageAccountNameTarget, destinationContainerName, destinationBlobName, destinationSasToken);

        // Perform GET and PUT operations
        InputStream blobInputStream = getBlob(sourceBlobUrl);
        putBlob(destinationBlobUrl, blobInputStream);
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

    private static void putBlob(String blobUrl, InputStream blobInputStream) throws Exception {
        URI uri = new URI(blobUrl);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);
        connection.setRequestProperty("x-ms-blob-type", "BlockBlob");

        byte[] buffer = new byte[8192];
        int bytesRead;
        try (OutputStream outStream = connection.getOutputStream()) {
            while ((bytesRead = blobInputStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, bytesRead);
            }
        }

        int responseCode = connection.getResponseCode();
        if (responseCode != 201) {
            throw new RuntimeException("Failed to upload blob: HTTP " + responseCode);
        }
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

