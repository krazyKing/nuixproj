package com.example.nuproj.service;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import parquet.hadoop.api.WriteSupport;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.awt.*;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Service
public class AmazonS3Service {
    public static final String WORKSPACE_PATH = "/Users/harsha/workspace/nuix/";
    public static final String WORKSPACE_PATH_RESULT = "/Users/harsha/workspace/nuix/result/";

    @Bean
    public void generateParquetFile() throws IOException {
        getS3Objects();
    }

    private void getS3Objects() throws IOException {
String key ="";
        AmazonS3 s3client = retrieveS3Client();

        ObjectListing objectListing = s3client.listObjects("candidate-33-s3-bucket");

        for (S3ObjectSummary os : objectListing.getObjectSummaries()) {
            key = os.getKey();
        }

        S3ObjectInputStream inputStream = s3client.getObject("candidate-33-s3-bucket", key).getObjectContent();
// retrieve object
        try {
            Files.copy(inputStream, Paths.get(new StringBuilder().append(WORKSPACE_PATH).append(key).toString()));
            File tmp = File.createTempFile(key, "");
            Files.copy(inputStream, tmp.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }

//unzip file
        unzipObject(key);
    }

    private void unzipObject(String key) throws IOException {
        File destDir = new File(WORKSPACE_PATH);
        byte[] buffer = new byte[1024];
        ZipInputStream zis = new ZipInputStream(new FileInputStream(WORKSPACE_PATH + key));
        ZipEntry zipEntry = zis.getNextEntry();
        while (zipEntry != null) {
            File newFile = newFile(destDir, zipEntry);
            if (zipEntry.isDirectory()) {
                if (!newFile.isDirectory() && !newFile.mkdirs()) {
                    throw new IOException("Failed to create directory " + newFile);
                }
            } else {
                File parent = newFile.getParentFile();
                if (!parent.isDirectory() && !parent.mkdirs()) {
                    throw new IOException("Failed to create directory " + parent);
                }
                // write file content
                FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
            }
            zipEntry = zis.getNextEntry();
        }
        zis.closeEntry();
        zis.close();

        File dir = new File(WORKSPACE_PATH);
        String[] files = dir.list();
        if (files.length == 0) {
            System.out.println("The directory is empty");
        } else {
            //convert each file to csv
            for (String aFile : files) {
                if (aFile.endsWith(".csv")) {

                    convertToCSVFile(aFile);
                }
            }
        }
    }

    private void convertToCSVFile(String fileName) {
        List<List<String>> result;
        try {
            result = Files.readAllLines(Paths.get(WORKSPACE_PATH + fileName))
                    .stream()
                    .map(line -> Arrays.asList(line.split(",")))
                    .collect(Collectors.toList());
            filterCSVFile(result, fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void filterCSVFile(List<List<String>> result, String fileName) throws IOException {
        FileWriter writer = new FileWriter(WORKSPACE_PATH + fileName);
        writer.write(String.join(",", result.get(0)));
        writer.append("\n");

        result.forEach(stringRow -> {
            stringRow.forEach(s -> {
                if (s.contains("ellipsis")) {
                    try {
                        writer.write(String.join(",", stringRow));
                        writer.append("\n");
                        writer.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        });

        prepareParquetFile(fileName);
    }

    private void prepareParquetFile(String fileName) throws IOException {

        uploadToS3(fileName);
    }

    private void uploadToS3(String fileName) {
        retrieveS3Client().putObject("candidate-33-s3-bucket", WORKSPACE_PATH, WORKSPACE_PATH+fileName);
    }

    private AmazonS3 retrieveS3Client() {
        AmazonS3ClientBuilder standard = AmazonS3ClientBuilder
                .standard();
        standard.withCredentials(
                new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(
                                "AKIAZUO64Q7BDDU4U7XL",
                                "TQ8da0iXYeFJMQG9TdeKGMdH+w3/u+BihbHen+JJ")));
        standard.withRegion("ap-southeast-2");
        return standard.build();
    }

    private File newFile(File destDir, ZipEntry zipEntry) throws IOException {
        File destFile = new File(destDir, zipEntry.getName());

        String destDirPath = destDir.getCanonicalPath();
        String destFilePath = destFile.getCanonicalPath();

        if (!destFilePath.startsWith(destDirPath + File.separator)) {
            throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
        }

        return destFile;
    }
}
