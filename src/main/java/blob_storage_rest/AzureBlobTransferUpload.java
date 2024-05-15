package blob_storage_rest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;

public class AzureBlobTransferUpload {

    public void azureBlobTransferUpload(Path localFilePath,BlobClient blobClient,long startTime) throws IOException{

        ParallelTransferOptions parallelTransferOptionsUpload = new ParallelTransferOptions()
         .setBlockSizeLong((long) (8 * 1024 * 1024));


     BlobUploadFromFileOptions blobUploadOptions = new BlobUploadFromFileOptions(localFilePath.toString());
        blobUploadOptions.setParallelTransferOptions(parallelTransferOptionsUpload);

        try {
            Response<BlockBlobItem> blockBlob = blobClient.uploadFromFileWithResponse(blobUploadOptions, null, null);
            System.err.println(blockBlob.getHeaders());
        } catch (UncheckedIOException ex) {
            System.err.printf("Failed to upload from file: %s%n", ex.getMessage());
        }

        System.out.println("Blob transferred and chunks uploaded successfully");
        Files.delete(localFilePath);

        long endTime = System.currentTimeMillis();

        // Print total time taken
        System.out.println("Total time taken: " + (endTime - startTime) / 60000.0 + " minutes");

}

}