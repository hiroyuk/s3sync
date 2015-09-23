package com.guremi.s3sync;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Data;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

import static com.amazonaws.services.s3.model.StorageClass.Glacier;
import static java.security.MessageDigest.getInstance;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 *
 * @author hiroyuk
 */
public class SimpleSync {

    private static final Logger LOG = Logger.getLogger(SimpleSync.class.getName());

    private Queue<Path> fileList(Path parentPath, Deque<Path> queue) {
        Queue<Path> fileList = new ConcurrentLinkedQueue<>();
        try (DirectoryStream<Path> subpathstream = Files.newDirectoryStream(parentPath)) {
            for (Path subpath : subpathstream) {
                if (Files.isDirectory(subpath)) {
                    queue.addLast(subpath);
                } else {
                    if (Files.size(subpath) < 10 * 1024 * 1024) {
                        fileList.add(subpath);
                    }
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(SimpleSync.class.getName()).log(Level.SEVERE, null, ex);
        }
        return fileList;
    }

    private String relativePath(Path rootPath, Path currentPath) {
        Path relPath = rootPath.relativize(currentPath);
        List<Path> pathnames = new ArrayList<>();
        relPath.iterator().forEachRemaining(pathnames::add);

        return pathnames.stream().map(p -> p.toString()).collect(joining("/"));
    }

    @Data
    class RemoteFile {

        String key;
        String etag;
        StorageClass storageClass;

        public RemoteFile(S3ObjectSummary sos) {
            key = sos.getKey();
            etag = sos.getETag();
            storageClass = StorageClass.fromValue(sos.getStorageClass());
        }
    }

    private Map<String, RemoteFile> createRemoteFileList(String bucket, String remoteRoot) {
        AmazonS3 s3client = new AmazonS3Client(new JsonAWSCredentials());

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);

        ObjectListing ol;
        Map<String, RemoteFile> map = new HashMap<>();
        do {
            ol = s3client.listObjects(request);
            map.putAll(
                    ol.getObjectSummaries().stream()
                    .filter(s -> remoteRoot == null || s.getKey().startsWith(remoteRoot))
                    .collect(toMap(s -> s.getKey(), RemoteFile::new)));
        } while (ol.isTruncated());

        LOG.info("remote file list created.");
        return map;
    }

    public void start() {

        Config config = ConfigFactory.load("config");
        ConfigList configList = config.getList("syncPair");

        CountDownLatch latch = new CountDownLatch(configList.size() * 2);
        ExecutorService executor = Executors.newFixedThreadPool(configList.size() * 2);

        configList.parallelStream().forEach(conf -> {
            Config c = ((ConfigObject) conf).toConfig();
            String remote = c.getString("remote");
            String local = c.getString("local");

            if (!remote.endsWith("/")) {
                remote = remote + "/";
            }
            if (remote.equals("/")) {
                remote = null;
            }

            Path rootPath = Paths.get(local);
            Deque<Path> queue = new ArrayDeque<>(Arrays.asList(rootPath));

            Map<String, RemoteFile> remoteFileMap = createRemoteFileList(c.getString("bucket"), remote);
            Queue<Path> localFileQueue = new ConcurrentLinkedQueue<>();

            while (!queue.isEmpty()) {
                Path current = queue.pollFirst();
                localFileQueue.addAll(this.fileList(current, queue));
            }
            LOG.info("local file list created.");

            for (int i = 0; i < 2; i++) {
                executor.execute(createWorker(localFileQueue, remoteFileMap, rootPath, c.getString("bucket"), remote, executor, latch));
            }
        });

        try {
            latch.await(1, TimeUnit.HOURS);
        } catch (InterruptedException ex) {
        }
        try {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
        }
    }

    private Runnable createWorker(Queue<Path> localFileQueue, Map<String, RemoteFile> remoteFileQueue, Path rootPath, String bucket, String remoteRoot, ExecutorService executor, CountDownLatch latch) {

        return () -> {
            while (!localFileQueue.isEmpty()) {
                Path current = localFileQueue.poll();
                if (current == null || executor.isShutdown()) {
                    break;
                }
                LOG.info(() -> String.format("file :%s", current));

                String pathname = relativePath(rootPath, current);
                if (remoteRoot != null) {
                    pathname = remoteRoot + pathname;
                }
                LOG.info(String.format("remote path name:%s", pathname));
                String md5 = md5digest(current, pathname, remoteFileQueue);
                
                if (md5 != null) {
                    PutObjectRequest por = new PutObjectRequest(bucket, pathname, current.toFile());
                    ObjectMetadata om = new ObjectMetadata();
                    om.setContentMD5(md5);

                    por.setStorageClass(StorageClass.StandardInfrequentAccess);
                    por.setMetadata(om);

                    AmazonS3 s3client = new AmazonS3Client(new JsonAWSCredentials());
                    TransferManager tm = new TransferManager(s3client);
                    try {
                        Upload upload = tm.upload(por);
                        LOG.info(String.format("upload start. file:%s, key:%s", current.toString(), pathname));

                        upload.waitForCompletion();
                        LOG.info(String.format("upload finish. file:%s, key:%s", current.toString(), pathname));
                    } catch (AmazonClientException | InterruptedException ex) {
                        LOG.log(Level.SEVERE, null, ex);
                    } finally {
                        tm.shutdownNow();
                    }
                }

            }
            latch.countDown();
        };
    }

    

    private String md5digest(Path localFile, String pathname, Map<String, RemoteFile> remoteFileQueue) {
        RemoteFile remoteFile = remoteFileQueue.get(pathname);
        if (remoteFile != null && remoteFile.storageClass == Glacier) {
            return null;
        }

        try {
            MessageDigest md = getInstance("MD5");
            try (InputStream is = Files.newInputStream(localFile)) {
                DigestInputStream dis = new DigestInputStream(is, md);
                while (dis.read() != -1) {}
                byte[] digest = dis.getMessageDigest().digest();

                String remoteMd5 = remoteFileQueue.containsKey(pathname) ? remoteFileQueue.get(pathname).etag : null;
                String base64digestText = new String(Base64.encodeBase64(digest));
                String hexDigestText = StringUtils.leftPad(new String(Hex.encodeHex(digest)), 32, '0');
                LOG.info(() -> String.format("md5 check.. local:%s, remote:%s", hexDigestText, remoteMd5));

                return hexDigestText.equals(remoteMd5) ? null : base64digestText;
            } catch (IOException ex) {
            }
        } catch (NoSuchAlgorithmException ex) {
        }
        return null;
    }

    public static void main(String[] args) {
        SimpleSync fl = new SimpleSync();
        fl.start();
    }
}
