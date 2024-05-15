package blob_storage_rest;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.options.BlobDownloadToFileOptions;
import com.azure.storage.common.ParallelTransferOptions;

public class AzureBlobTransferDownload {
    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();

        String sourceConnectionString = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
        String targetConnectionString = System.getenv("AZURE_STORAGE_CONNECTION_STRING_TARGET");
        String sourceContainerName = "large-blob";
        String targetContainerName = "target-container";
        String sourceBlobName = "100MB.zip";

        BlobServiceClient sourceClient = new BlobServiceClientBuilder().connectionString(sourceConnectionString)
                .buildClient();
        BlobServiceClient targetClient = new BlobServiceClientBuilder().connectionString(targetConnectionString)
                .buildClient();
        BlobContainerClient sourceContainer = sourceClient.getBlobContainerClient(sourceContainerName);
        BlobContainerClient targetContainer = targetClient.getBlobContainerClient(targetContainerName);
        BlobClient sourceBlob = sourceContainer.getBlobClient(sourceBlobName);

        BlobClient targetBlobClient = targetContainer.getBlobClient(sourceBlobName);
        if (targetBlobClient.exists()) {
            System.out.println("Target blob already exists.");
            return; // Exit the program 
        }

        Path localFilePath = Paths.get(sourceBlobName);
        String pathString = sourceBlobName;

        ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
        .setBlockSizeLong((long) (8 * 1024 * 1024));

        BlobDownloadToFileOptions blobDownloadOptions = new BlobDownloadToFileOptions(pathString);
        blobDownloadOptions.setParallelTransferOptions(parallelTransferOptions);

        sourceBlob.downloadToFileWithResponse(blobDownloadOptions, null, null);

        String fileName = localFilePath.getFileName().toString();
        BlobClient blobClient = targetContainer.getBlobClient(fileName);

       AzureBlobTransferUpload azureBlobTransferUploadObject = new AzureBlobTransferUpload();
       azureBlobTransferUploadObject.azureBlobTransferUpload(localFilePath,blobClient,startTime);
       
    }
}