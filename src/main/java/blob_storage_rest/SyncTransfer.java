package blob_storage_rest;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SyncTransfer {
    public static void main(String[] args) {
        // Source string, container name and blob name
        String sourceConnectionString = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
        String sourceContainerName = "large-blob";
        String sourceBlobName = "100MB.zip";

        // Target connection string and container name
        String targetConnectionString = System.getenv("AZURE_STORAGE_CONNECTION_STRING_TARGET");
        String targetContainerName = "target-container";

        // Start time measurement
        long startTime = System.currentTimeMillis();

        // Create source and target BlobServiceClients
        BlobServiceClient sourceClient = new BlobServiceClientBuilder()
                .connectionString(sourceConnectionString)
                .buildClient();
        BlobServiceClient targetClient = new BlobServiceClientBuilder()
                .connectionString(targetConnectionString)
                .buildClient();

        // Get source and target containers
        BlobContainerClient sourceContainer = sourceClient.getBlobContainerClient(sourceContainerName);
        BlobContainerClient targetContainer = targetClient.getBlobContainerClient(targetContainerName);

        // Get source blob
        BlobClient sourceBlob = sourceContainer.getBlobClient(sourceBlobName);

        // Define chunk size
        long chunkSize = 4 * 1024 * 1024L; 

        // Download source blob and upload chunks to target
        try (InputStream inputStream = sourceBlob.openInputStream()) {
            List<byte[]> chunks = new ArrayList<>();
            int chunkCount = 0;
            int bytesRead;
            byte[] buffer = new byte[(int) chunkSize];
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                byte[] chunk = new byte[bytesRead];
                System.arraycopy(buffer, 0, chunk, 0, bytesRead);
                chunks.add(chunk);
                chunkCount++;
                System.out.println("Chunk " + chunkCount + " transferred.");
            }
            // Upload chunks to target in parallel
            AtomicInteger counter = new AtomicInteger(0);
            chunks.parallelStream().forEach(chunk -> {
                uploadChunkToTarget(chunk, targetContainer, sourceBlobName, counter.getAndIncrement());
            });
            
            // Commit the block list to finalize the blob upload
            commitBlockList(targetContainer, sourceBlobName, chunks.size());
            
            System.out.println("Blob transfer completed.");
        } catch (IOException e) {
            System.err.println("An error occurred during blob transfer: " + e.getMessage());
            e.printStackTrace();
        }


        // End time measurement
        long endTime = System.currentTimeMillis();

        // Print total time taken
        System.out.println("Total time taken: " + (endTime - startTime) / 60000.0 + " minutes");
    }

    private static void uploadChunkToTarget(byte[] chunk, BlobContainerClient targetContainer, String blobName, int index) {
        try {
            BlockBlobClient targetBlob = targetContainer.getBlobClient(blobName).getBlockBlobClient();
            try (InputStream chunkInputStream = new ByteArrayInputStream(chunk)) {
                // Generate block ID
                String blockId = Base64.getEncoder().encodeToString(String.format("%06d", index).getBytes());
                // Upload chunk
                targetBlob.stageBlock(blockId, chunkInputStream, chunk.length);
                System.out.println("Chunk uploaded to target with index: " + index);
            }
        } catch (IOException e) {
            throw new RuntimeException("An error occurred during chunk upload: " + e.getMessage(), e);
        }
    }
    
    private static void commitBlockList(BlobContainerClient targetContainer, String blobName, int chunkCount) {
        try {
            BlockBlobClient targetBlob = targetContainer.getBlobClient(blobName).getBlockBlobClient();
            List<String> blockIds = new ArrayList<>();
            for (int i = 0; i < chunkCount; i++) {
                blockIds.add(Base64.getEncoder().encodeToString(String.format("%06d", i).getBytes()));
            }
            targetBlob.commitBlockList(blockIds);
            System.out.println("Block list committed.");
        } catch (Exception e) {
            throw new RuntimeException("An error occurred during committing block list: " + e.getMessage(), e);
        }
    }
}
