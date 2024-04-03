package com.run;

import com.run.util.ConfigUtil;
import io.minio.MinioClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 数据老化
 *
 * @author liyanhong
 */
public class DataCleaner {

    public static Logger log = LoggerFactory.getLogger(DataCleaner.class);

    public static void main(String[] args) {
        try {
            String hostIp = ConfigUtil.getHostIp();
            log.info("<conf> hostIp={}", hostIp);
            log.info("<conf> data.retention.days={}", ConfigUtil.getProperty("data.retention.days", "30"));
            log.info("<conf> minio.url={}", ConfigUtil.getProperty("minio.url"));
            log.info("<conf> minio.username={}", ConfigUtil.getProperty("minio.username"));
            log.info("<conf> minio.password=#{}", ConfigUtil.getProperty("minio.password").length());

            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                deleteBackupFiles();
            }, ConfigUtil.getInitialDelay(1, 0, 0), 24 * 60 * 60 * 1000L, TimeUnit.MILLISECONDS);
            //}, 0, 1, TimeUnit.DAYS);

        } catch (Exception e) {
            log.error("exec main fail", e);
        }
    }

    private static void deleteBackupFiles() {
        int days = Integer.parseInt(ConfigUtil.getProperty("data.retention.days", "30"));
        String minioUrl = ConfigUtil.getProperty("minio.url");
        String minioUser = ConfigUtil.getProperty("minio.username");
        String minioPassword = ConfigUtil.getProperty("minio.password");
        MinioClient client = MinioClient.builder().endpoint(minioUrl) // api地址
                .credentials(minioUser, minioPassword) // 前面设置的账号密码
                .build();
        MinioClientTool minioTool = new MinioClientTool(client);
        minioTool.deleteBuckets(days);
    }

}
