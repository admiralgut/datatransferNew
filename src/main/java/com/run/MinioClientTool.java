package com.run;

import com.google.common.base.Stopwatch;
import com.run.util.ConfigUtil;
import com.run.util.RunFileUtils;
import com.run.util.StringUtil;
import io.minio.*;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author liyanhong
 */
public class MinioClientTool {

    private static final Logger log = LoggerFactory.getLogger(MinioClientTool.class);

    private final MinioClient client;

    public MinioClientTool(MinioClient client) {
        this.client = client;
    }

    public static void cleanLocalFiles(File dir, int days) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        log.info("cleanLocalFiles begin: days=" + days + ", dir=" + dir.getAbsolutePath());
        LocalDate now = LocalDate.now();
        List<String> dirs = getDeleteLocalDirs(dir, now, days);
        for (String dd : dirs) {
            Stopwatch stopwatch2 = Stopwatch.createStarted();
            File fdir = new File(dir, dd);
            // FileUtils.deleteQuietly(fdir);
            RunFileUtils.deleteQuietly(fdir);
            log.info("deleteLocalDir ok: dir={}, takes={}", fdir.getAbsolutePath(), stopwatch2.elapsed());
        }
        log.info("cleanLocalFiles end: days=" + days + ", dir=" + dir.getAbsolutePath() + ", takes=" + stopwatch.elapsed());
    }

    public static List<String> getDeleteLocalDirs(File dir, LocalDate now, int days) {
        List<String> result = Lists.newArrayList();
        if (!dir.exists() || !dir.isDirectory()) {
            return result;
        }

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
        int lowBound = Integer.valueOf(now.plusDays(-days).format(dtf));
        log.info("getDeleteLocalDirs# now=" + now.format(dtf) + ", days=" + days + ", lowBound=" + lowBound);

        File[] files = dir.listFiles();
        for (File f : files) {
            if (f.isDirectory() && StringUtil.isNumber(f.getName(), 8)) {
                if (Integer.parseInt(f.getName()) < lowBound) {
                    result.add(f.getName());
                }
            }
        }
        Collections.sort(result);
        log.info("getDeleteLocalDirs# total={}, result={}", files.length, result);
        return result;
    }

    public void deleteBuckets(int days) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        log.info("deleteBuckets begin: days=" + days);
        List<String> buckets = getDeleteBuckets(LocalDate.now(), days);
        for (String bucketName : buckets) {
            deleteBucket(bucketName);
        }
        log.info("deleteBuckets end: days=" + days + ", takes=" + stopwatch.elapsed());
    }

    public void deleteBucket(String bucketName) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long count = 0;
        try {
            boolean found = client.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                return;
            }

            ListObjectsArgs args = ListObjectsArgs.builder().bucket(bucketName).recursive(true).build();
            Iterable<Result<Item>> iter = client.listObjects(args);

            for (Result<Item> x : iter) {
                Item item = x.get();
                if (item.isDir()) {
                    log.info("delete bucket diritem: bucket=" + bucketName + ", item=" + item.objectName());
                } else {
                    client.removeObject(RemoveObjectArgs.builder().object(item.objectName()).bucket(bucketName).build());
                    count++;
                }
            }
            client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
            log.info("delete bucket ok: bucket=" + bucketName + ", count=" + count + ", takes=" + stopwatch.elapsed());
        } catch (Exception e) {
            log.error("delete bucket failed: bucket=" + bucketName+ ", count=" + count + ", takes=" + stopwatch.elapsed(), e);
        }
    }

    public List<String> getDeleteBuckets(LocalDate now, int days) {
        String hostIp = ConfigUtil.getHostIp();
        List<String> result = Lists.newArrayList();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyMMdd");
        int lowBound = Integer.valueOf(now.plusDays(-days).format(dtf));
        log.info("getDeleteBuckets# hostIp=" + hostIp + ", now=" + now.format(dtf) + ", days=" + days + ", lowBound=" + lowBound);
        try {
            List<Bucket> buckets = client.listBuckets();
            for (Bucket b : buckets) {
                String bucketName = b.name();
                if (!bucketName.startsWith("chan-")) {
                    continue;
                }
                int bucketDate = Integer.parseInt(bucketName.substring(b.name().length() - 6));
                if (bucketDate < lowBound) {
                    boolean found = client.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
                    if (found) {
                        result.add(bucketName);
                    }
                }
            }
        } catch (Exception e) {
            log.error("getDeleteBuckets# failed", e);
        }
        if (!result.isEmpty()) {

            Collections.sort(result, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    String[] arr1 = o1.split("-");
                    String[] arr2 = o2.split("-");
                    if (arr1.length != 4 || arr1.length != arr2.length) {
                        return 1;
                    }
                    if (arr1[1].equals(hostIp)) {
                        if (arr2[1].equals(hostIp)) {
                            return o1.compareTo(o2);
                        } else {
                            return -1;
                        }
                    } else if (arr2[1].equals(hostIp)) {
                        return 1;
                    } else {
                        return o1.compareTo(o2);
                    }
                }
            });
        }
        log.info("getDeleteBuckets# result={}", result);
        return result;
    }

}
