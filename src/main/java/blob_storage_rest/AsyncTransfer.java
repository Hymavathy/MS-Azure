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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncTransfer {
    public static void main(String[] args) {
        //source string, container name and blob name
        String sourceConnectionString = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
        String sourceContainerName = "large-blob";
        String sourceBlobName = "100MB.zip";

        // Target connection string and container name
        String targetConnectionString = System.getenv("AZURE_STORAGE_CONNECTION_STRING_TARGET");
        String targetContainerName = "target-container";
        String targetBlobName = sourceBlobName; 

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

        // Check if the target blob already exists
        BlobClient targetBlobClient = targetContainer.getBlobClient(targetBlobName);
        if (targetBlobClient.exists()) {
            System.out.println("Target blob already exists.");
            return; // Exit the program 
        }

        // Get source blob
        BlobClient sourceBlob = sourceContainer.getBlobClient(sourceBlobName);

        // Defining chunk size
        long chunkSize = 8 * 1024 * 1024L; 

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
            // Upload chunks to target
            uploadChunksToTargetInParallel(chunks, targetContainer, targetBlobName);
            System.out.println("Blob transfer completed.");
        } catch (IOException e) {
            System.err.println("An error occurred during blob transfer: " + e.getMessage());
            e.printStackTrace();
        }

        // End time measurement
        long endTime = System.currentTimeMillis();

        // Printing total time taken
        System.out.println("Total time taken: " + (endTime - startTime) / 60000.0 + " minutes");
    }

    private static void uploadChunksToTargetInParallel(List<byte[]> chunks, BlobContainerClient targetContainer, String blobName) {
        try {
            BlockBlobClient targetBlob = targetContainer.getBlobClient(blobName).getBlockBlobClient();

            ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

            for (int i = 0; i < chunks.size(); i++) {
                final int index = i;
                byte[] chunk = chunks.get(i);
                executor.execute(() -> {
                    try (InputStream chunkInputStream = new ByteArrayInputStream(chunk)) {
                        String blockId = Base64.getEncoder().encodeToString(String.format("%06d", index).getBytes());
                        targetBlob.stageBlock(blockId, chunkInputStream, chunk.length);
                        System.out.println("Chunk " + (index + 1) + " uploaded to target.");
                    } catch (IOException e) {
                        e.printStackTrace(); 
                    }
                });
            }

            executor.shutdown();
            while (!executor.isTerminated()) {
                // Waiting for all tasks to finish
            }

            List<String> blockIds = new ArrayList<>();
            for (int i = 0; i < chunks.size(); i++) {
                blockIds.add(Base64.getEncoder().encodeToString(String.format("%06d", i).getBytes()));
            }
            targetBlob.commitBlockList(blockIds);
        } catch (Exception e) {
            throw new RuntimeException("An error occurred during chunk upload: " + e.getMessage(), e);
        }
    }
}
