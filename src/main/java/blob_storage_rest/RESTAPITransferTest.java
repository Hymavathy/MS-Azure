package blob_storage_rest;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.HttpsURLConnection;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class RESTAPITransferTest {
    private static final String SOURCE_STORAGE_CONNECTION_STRING = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
    private static final String DEST_STORAGE_CONNECTION_STRING = System
            .getenv("AZURE_STORAGE_CONNECTION_STRING_TARGET");
    private static final String SOURCE_CONTAINER_NAME = "large-blob";
    private static final String TARGET_CONTAINER_NAME = "target-container";
    private static final String BLOB_NAME = "UniversityTimeTable.jpeg";
    private static final int CHUNK_SIZE = 4 * 1024 * 1024; // 4 MB

    public static void main(String[] args)
            throws IOException, URISyntaxException, InvalidKeyException, NoSuchAlgorithmException {
        // Download data in chunks
        byte[] downloadedData = downloadChunks(SOURCE_STORAGE_CONNECTION_STRING);

        // Upload data in chunks
        uploadChunks(downloadedData, DEST_STORAGE_CONNECTION_STRING);
    }

    private static byte[] downloadChunks(String connectionString)
            throws IOException, URISyntaxException, InvalidKeyException, NoSuchAlgorithmException {
        String urlString = getUrlFromConnectionString(connectionString) + "/" + SOURCE_CONTAINER_NAME + "/" + BLOB_NAME;
        try {
            HttpsURLConnection connection = (HttpsURLConnection) new URI(urlString).toURL().openConnection();

            connection.setRequestMethod("GET");
            connection.setRequestProperty("Authorization",
                    getAuthorizationHeaderForGet(connectionString, SOURCE_CONTAINER_NAME));
            connection.setRequestProperty("x-ms-date", getServerDateTime());
            connection.setRequestProperty("x-ms-version", "2020-04-08");

            int responseCode = connection.getResponseCode();
            if (responseCode != HttpsURLConnection.HTTP_OK) {
                System.err.println("Failed to connect. Server returned HTTP response code: " + responseCode);
                return null;
            }

            try (InputStream inputStream = connection.getInputStream()) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                byte[] buffer = new byte[CHUNK_SIZE];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
                return outputStream.toByteArray();
            }
        } catch (IOException | URISyntaxException e) {
            System.err.println("Failed to establish connection: " + e.getMessage());
            throw e;
        }
    }

    private static String getAuthorizationHeaderForGet(String connectionString, String containerName)
            throws InvalidKeyException, NoSuchAlgorithmException {
        String accountName = getAccountNameFromConnectionString(connectionString);
        String accountKey = getAccountKeyFromConnectionString(connectionString);
        String method = "GET";
        String contentEncoding = "";
        String contentLanguage = "";
        String contentLength = "";
        String contentMD5 = "";
        String contentType = "";
        String date = "";

        String x_ms_date = getServerDateTime();
        String x_ms_version = "2020-04-08";

        String canonicalizedHeaders = String.format("x-ms-date:%s\nx-ms-version:%s", x_ms_date, x_ms_version);
        String canonicalizedResource = String
                .format("/%s/%s/%s", accountName, containerName, BLOB_NAME);

        String stringToSign = method + "\n" +
                contentEncoding + "\n" +
                contentLanguage + "\n" +
                contentLength + "\n" +
                contentMD5 + "\n" +
                contentType + "\n" +
                date + "\n" +
                "" + "\n" + // ifModifiedSince
                "" + "\n" + // ifMatch
                "" + "\n" + // ifNoneMatch
                "" + "\n" + // ifUnmodifiedSince
                "" + "\n" + // range
                canonicalizedHeaders + "\n" +
                canonicalizedResource;

        System.out.println("STRING GET\n" + stringToSign);

        String signature = getSharedKeySignature(accountKey, stringToSign);
        System.out.println("SharedKey " + accountName + ":" + signature);

        return "SharedKey " + accountName + ":" + signature;

    }

    private static void uploadChunks(byte[] data, String connectionString)
            throws IOException, URISyntaxException, InvalidKeyException, NoSuchAlgorithmException {
        String urlString = getUrlFromConnectionString(connectionString) + "/" + TARGET_CONTAINER_NAME + "/" + BLOB_NAME;
        String contentLength = String.valueOf(data.length);
        System.out.println("blob length in bytes : "+contentLength);

        try {

            HttpsURLConnection connection = (HttpsURLConnection) new URI(urlString).toURL().openConnection();

            // Set request headers
            connection.setRequestMethod("PUT");

            connection.setRequestProperty("Authorization",
                    getAuthorizationHeaderForPut(connectionString, TARGET_CONTAINER_NAME,data.length));
            connection.setDoOutput(true);
            connection.setRequestProperty("x-ms-date", getServerDateTime());
            connection.setRequestProperty("x-ms-blob-type", "BlockBlob");
            connection.setRequestProperty("x-ms-version", "2020-04-08");

            
            // Upload data
            try (OutputStream outputStream = connection.getOutputStream()) {

                outputStream.write(data);
                outputStream.flush();

                System.out.println("Content length value:"+connection.getRequestProperty("Content-Length"));


            }
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpsURLConnection.HTTP_OK) {
                System.err.println("Failed to connect. Server returned HTTP response code: " + responseCode+":"+connection.getResponseMessage());
                return;
            }

            if (responseCode / 100 != 2) {
                System.err.println("Upload failed with response code: " + responseCode);
            } else {
                System.out.println("Upload successful!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getAuthorizationHeaderForPut(String connectionString, String containerName,long lengthString)
            throws InvalidKeyException, NoSuchAlgorithmException {
                String accountName = getAccountNameFromConnectionString(connectionString);
                String accountKey = getAccountKeyFromConnectionString(connectionString);
                String method = "PUT";
                String contentEncoding = "";
                String contentLanguage = "";
                String contentLength = "";
                String contentMD5 = "";
                String contentType = "";
                String date = "";
                String blobType = "";
        
                String x_ms_date = getServerDateTime();
                String x_ms_version = "2020-04-08";
                String canonicalizedHeaders = String.format(
                                        "\nx-ms-date:%s\nx-ms-blob-type:%s\nx-ms-version:%s",x_ms_date,blobType,x_ms_version);
                String canonicalizedResource = String
                        .format("/%s/%s/%s\ntimeout:1200", accountName, containerName,BLOB_NAME);
        
                String stringToSign = method + "\n" +
                        contentEncoding + "\n" +
                        contentLanguage + "\n" +
                        contentLength + "\n" +
                        contentMD5 + "\n" +
                        contentType + "\n" +
                        date + "\n" +
                        "" + "\n" + // ifModifiedSince
                        "" + "\n" + // ifMatch
                        "" + "\n" + // ifNoneMatch
                        "" + "\n" + // ifUnmodifiedSince
                        "" + "\n" + // range
                        canonicalizedHeaders + "\n" +
                        canonicalizedResource;
        
                System.out.println("STRING PUT\n" + stringToSign);
        
                String signature = getSharedKeySignature(accountKey, stringToSign);
                System.out.println("SharedKey " + accountName + ":" + signature);
        
                return "SharedKey " + accountName + ":" + signature;

    }

    private static String getSharedKeySignature(String accountKey, String stringToSign)
            throws InvalidKeyException, NoSuchAlgorithmException {
        // Decode the account shared key from base64.
        byte[] decodedKey = Base64.getDecoder().decode(accountKey);

        // Create an HMAC-SHA256 object using the decoded key.
        Mac hmacsha256 = Mac.getInstance("HmacSHA256");
        hmacsha256.init(new SecretKeySpec(decodedKey, "HmacSHA256"));

        // Sign the UTF-8 encoded string.
        byte[] signatureBytes = hmacsha256.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8));

        // Base64 encode the signature bytes.
        return Base64.getEncoder().encodeToString(signatureBytes);
    }

    private static String getServerDateTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf.format(new Date());
    }

    private static String getUrlFromConnectionString(String connectionString) {
        String[] parts = connectionString.split(";");
        for (String part : parts) {
            if (part.startsWith("DefaultEndpointsProtocol=")) {
                String protocol = part.split("=")[1];
                for (String otherPart : parts) {
                    if (otherPart.startsWith("AccountName=")) {
                        String accountName = otherPart.split("=")[1];
                        return protocol + "://" + accountName + ".blob.core.windows.net";
                    }
                }
            }
        }
        throw new IllegalArgumentException("Invalid connection string");
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
