package com.run;

import com.google.common.base.Stopwatch;
import com.run.util.ConfigUtil;
import com.run.util.FileUtil;
import com.run.util.Ssh2Util;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataTransfer {
    public static Logger log = LoggerFactory.getLogger(DataTransfer.class);
    public static int SCAN_INTERVAL = 500;
    public static int SEND_INTERVAL = 500;
    public static MinioClient client;
    public static String PATH_TO_LOG;
    public static String BACKUP_LOCAL_PATH;
    public static int DAYS;
    public static boolean BACKUP_ENABLE = false;
    public static boolean QUOTA_ENABLE = false;
    public static boolean VALIDATE_ENABLE = false;
    public static boolean SCP_ENABLE = false;
    public static String HOST_IP;
    static int SCAN_FILE_COUNT = 10000;
    static long LOG_SWAP_SECONDS = 600;

    static long LOG_SWAP_MINUTES = 10;
    private static int SCAN_POOL_SIZE = 1;
    private static int SEND_POOL_SIZE = 10;

    public static void init() {
        HOST_IP = ConfigUtil.getHostIp();
        SCAN_POOL_SIZE = ConfigUtil.getInt("scan.pool.size");
        SCAN_FILE_COUNT = ConfigUtil.getInt("scan.file.count");
        SEND_POOL_SIZE = ConfigUtil.getInt("send.pool.size");
        SCAN_INTERVAL = ConfigUtil.getInt("scan.interval.milliseconds");
        SEND_INTERVAL = ConfigUtil.getInt("send.interval.milliseconds");
        PATH_TO_LOG = ConfigUtil.getProperty("transfer.log.path");
        BACKUP_LOCAL_PATH = ConfigUtil.getProperty("backup.local.path");
        DAYS = ConfigUtil.getInt("data.retention.days");
        BACKUP_ENABLE = ConfigUtil.getBoolean("backup.enable");
        QUOTA_ENABLE = ConfigUtil.getBoolean("quota.enable");
        VALIDATE_ENABLE = ConfigUtil.getBoolean("file.validate.enable");
        SCP_ENABLE = ConfigUtil.getBoolean("scp.enable");
        LOG_SWAP_SECONDS = ConfigUtil.getLong("transfer.log.upload.interval.minutes") * 60;
        LOG_SWAP_MINUTES = ConfigUtil.getLong("transfer.log.upload.interval.minutes");

        if (BACKUP_ENABLE) {
            client = MinioClient.builder().endpoint(ConfigUtil.getProperty("minio.url")) // api地址
                    .credentials(ConfigUtil.getProperty("minio.username"), ConfigUtil.getProperty("minio.password")) // 前面设置的账号密码
                    .build();
        }
    }

    public static void main(String[] args) {
        try {
            init();
            recoveryCheckpoints();
            ConcurrentLinkedQueue<FileItem> queue = new ConcurrentLinkedQueue<FileItem>();
            List<Chan> chans = getChannels();
            if (QUOTA_ENABLE) {
                updateUsed(chans);
            }

            List<Chan>[] tasks = assignTasks(chans);
            ExecutorService pool = Executors.newFixedThreadPool(SCAN_POOL_SIZE);
            for (int i = 0; i < SCAN_POOL_SIZE; i++) {
                pool.execute(new ScanTask(queue, tasks[i]));
            }

            ExecutorService sendPool = Executors.newFixedThreadPool(SEND_POOL_SIZE);
            for (int i = 0; i < SEND_POOL_SIZE; i++) {
                sendPool.execute(new SendTask(i, queue));
            }

            if (BACKUP_ENABLE) {
                ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

                scheduledExecutorService.scheduleAtFixedRate(() -> {
                    deleteBackupFiles(chans, DAYS);
                }, ConfigUtil.getInitialDelay(1, 0, 0), 24 * 60 * 60 * 1000L, TimeUnit.MILLISECONDS);
                //}, 0, 1, TimeUnit.DAYS);
            }
        } catch (Exception e) {
            log.error("exec main fail", e);
        }
    }

    private static void deleteBackupFiles(List<Chan> chans, int days) {
//        MinioClientTool minioTool = new MinioClientTool(client);
//        minioTool.deleteBuckets(days);

        log.info("deleteLocalFiles begin: days=" + days + ", chans.size=" + chans.size());
        for (Chan chan : chans) {
            MinioClientTool.cleanLocalFiles(chan.getSrc().getErrDir(), days);
        }
        log.info("deleteLocalFiles end: days=" + days + ", chans.size=" + chans.size());
    }

    private static void recoveryCheckpoints() {
        log.info("recoveryCheckpoints# start");
        File dir = new File(DataTransfer.PATH_TO_LOG);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        if (dir.isDirectory()) {
            File[] logs = dir.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.getName().endsWith(".temp");
                }
            });
            log.info("recoveryCheckpoints# statDir={}, tempFiles={}", dir.getAbsolutePath(), logs.length);
            for (File f : logs) {
                if (f.length() > 0) {
                    String name = f.getName();
                    String newname = name.substring(0, name.length() - 5);
                    int idx = newname.lastIndexOf("_");
                    long ts = Long.parseLong(newname.substring(idx + 1));
                    File newFile;
                    do {
                        ts = ts + 1;
                        newname = newname.substring(0, idx + 1) + ts + ".log";
                        newFile = new File(f.getParentFile(), newname);
                    } while (newFile.exists());

                    f.renameTo(newFile);
                    log.info("recovery stat file ok: rename {} to {}", f.getAbsolutePath(), newFile.getAbsolutePath());
                } else {
                    f.delete();
                    log.info("recovery stat file ok: delete empty file {}", f.getAbsolutePath());
                }
            }
        }
        log.info("recoveryCheckpoints# end");
    }

    private static List<Chan> getChannels() {
        List<Chan> chanConfs = new ArrayList<>();

        for (String name : ConfigUtil.getPropertyNames()) {
            if (!name.startsWith("channel.")) {
                continue;
            }
            String val = ConfigUtil.getProperty(name);
            String[] sa = name.split("\\.");
            if (sa.length < 3) {
                log.error("config error: {}", name);
                throw new RuntimeException("config error!");
            }
            String chanId = sa[1];
            Chan chan = null;
            for (Chan ch : chanConfs) {
                if (ch.id.equals(chanId)) {
                    chan = ch;
                }
            }
            if (chan == null) {
                chan = new Chan(chanId);
                chanConfs.add(chan);
            }

            String sinkId = null;
            String dirId = null;

            if (name.contains(".sink.")) {
                if (sa.length < 6) {
                    log.error("config error: {}", name);
                    throw new RuntimeException("config error!");
                }
                sinkId = sa[3];
                Sink sink = chan.getSinkById(sinkId);
                if (name.endsWith(".validate.enable")) {
                    sink.setValidate(Boolean.parseBoolean(val));
                    continue;
                }
                dirId = sa[5];
                SinkDir sinkDir = sink.getSinkDirById(dirId);
                if (name.endsWith(".quota")) {
                    String unit = val.substring(val.length() - 2);
                    if (!unit.equalsIgnoreCase("TB") && !unit.equalsIgnoreCase("GB") && !unit.equalsIgnoreCase("MB")) {
                        log.error("config error: {{}}, must in [TB,GB,MB].", name);
                        throw new RuntimeException("config error!");
                    } else {
                        sinkDir.setQuota(Long.valueOf(val.substring(0, val.length() - 2)) * getUnitValue(unit));
                    }
                } else if (name.endsWith(".ip")) {
                    sinkDir.setIp(val);
                } else if (sa.length == 6 && sa[4].equals("dir")) {
                    val = StringUtils.stripEnd(val, "\\/");
                    sinkDir.setDir(val);
                } else {
                    log.warn("<config> unknow property: {} = {}", name, val);
                }
            } else if (name.endsWith(".source")) {
                String keyPrefix = name; //name.substring(0, name.length() - 7);
                chan.setSrc(new Source(DataTransfer.HOST_IP, val));
                if (ConfigUtil.hasProperty(keyPrefix + ".subpath.num")) {
                    chan.getSrc().setSubpathNum(ConfigUtil.getInt(keyPrefix + ".subpath.num"));
                }
                if (ConfigUtil.hasProperty(keyPrefix + ".type")) {
                    chan.getSrc().setType(ConfigUtil.getProperty(keyPrefix + ".type"));
                }
            } else if (name.endsWith(".backup")) {
                chan.setBackup(Boolean.valueOf(val));
            } else if (name.endsWith(".match.regex")) {
                chan.setMatchRegex(ConfigUtil.getProperty(name));
            } else if (name.endsWith(".nomatch.regex")) {
                chan.setNoMatchRegex(ConfigUtil.getProperty(name));
            }
        }
        StringBuilder sb = new StringBuilder("-----Channels------");
        sb.append(chanConfs.size());
        for (Chan chan : chanConfs) {
            Collections.sort(chan.getSinks(), new Comparator<Sink>() {
                @Override
                public int compare(Sink o1, Sink o2) {
                    if (o1.isValidate() == o2.isValidate()) {
                        return o1.getId().compareTo(o2.getId());
                    } else if (o1.isValidate()) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            });
            for (Sink sink : chan.getSinks()) {
                if (sink.dirList == null || sink.dirList.isEmpty()) {
                    throw new RuntimeException("Channel sink dir is empty: " + chan);
                }
                Collections.sort(sink.dirList, new Comparator<SinkDir>() {
                    @Override
                    public int compare(SinkDir o1, SinkDir o2) {
                        return o1.id.compareTo(o2.id);
                    }
                });
            }
            sb.append('\n').append(chan);
        }
        log.info(sb.toString());
        return chanConfs;
    }

    public static long getUnitValue(String unit) {
        switch (unit.toUpperCase()) {
            case "TB":
                return FileUtil.ONE_TB;
            case "GB":
                return FileUtil.ONE_GB;
            case "MB":
                return FileUtil.ONE_MB;
        }
        return -1;
    }

    public static List<Chan>[] assignTasks(List<Chan> chans) {
        if (SCAN_POOL_SIZE > chans.size()) {
            SCAN_POOL_SIZE = chans.size();
        }
        List<Chan>[] tasks = new List[SCAN_POOL_SIZE];
        for (int i = 0; i < SCAN_POOL_SIZE; i++) {
            tasks[i] = new ArrayList<>();
        }

        for (int i = 0; i < chans.size(); i++) {
            tasks[i % SCAN_POOL_SIZE].add(chans.get(i));
        }
        return tasks;
    }

    public static void updateUsed(List<Chan> chans) {
        for (Chan ch : chans) {
            for (Sink sink : ch.getSinks()) {
                for (SinkDir sinkDir : sink.dirList) {
                    File dir = new File(sinkDir.dir);
                    if (!dir.exists()) {
                        sinkDir.used.set(0L);
                    } else {
                        long used = FileUtil.getUsedByCmd(dir.getAbsolutePath());//FileUtils.sizeOfDirectory(dir);
                        sinkDir.used.set(used);
                    }
                }
            }
        }

    }

    public static long minute_fromt(long millis) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(millis);
        int minute = calendar.get(Calendar.MINUTE);
        // minute = Math.round(minute / 10 * 10);
        minute = Math.round(minute / LOG_SWAP_MINUTES * LOG_SWAP_MINUTES);
        calendar.set(Calendar.MINUTE, minute);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime().getTime() / 1000;
    }

    public static synchronized void createBucketIfNotExists(String bucketName) {
        try {
            boolean found = client.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                log.info("create Bucket: " + bucketName);
            }
        } catch (Exception e) {
            log.error("failed to create bucket: {} ", bucketName);
            e.printStackTrace();
        }

    }

    public static boolean checkObjectExists(String bucketName, String objectName) {
        try {
            client.statObject(StatObjectArgs.builder().bucket(bucketName).object(objectName).build());
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static class ScanTask implements Runnable {
        ConcurrentLinkedQueue<FileItem> queue;
        List<Chan> chans;

        public ScanTask(ConcurrentLinkedQueue<FileItem> queue, List<Chan> chans) {
            this.queue = queue;
            this.chans = chans;
        }

        @Override
        public void run() {
            while (true) {
                for (Chan chan : chans) {
                    //DataTransfer.log.info("starting scan directory: {}", chan.getSrc());
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    String chanId = chan.getId();
                    Source src = chan.getSrc();
                    List<Sink> sinks = chan.getSinks();
                    File srcFolder = new File(src.getDir());
                    if (!srcFolder.exists() || !srcFolder.isDirectory()) {
                        DataTransfer.log.error("scan channel: takes={}, chanId={}, srcDir={}, dir is not found!", stopwatch.elapsed(), chanId, srcFolder);
                        continue;
                    }
                    //make error directory
//                    File errorDir = new File(srcFolder.getParent(), srcFolder.getName() + "_error");
//                    if (!errorDir.exists()) {
//                        errorDir.mkdirs();
//                    }
                    AtomicInteger count = new AtomicInteger(0);
                    scanFiles(chan, src, sinks, srcFolder, queue, count);
                    DataTransfer.log.info("scan channel: takes={}, chanId={}, srcDir={}, count={}", stopwatch.elapsed(), chanId, srcFolder, count);
                }
                try {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    int beforeSize = queue.size();
                    while (!queue.isEmpty()) {
                        Thread.sleep(DataTransfer.SCAN_INTERVAL);
                    }
                    Thread.sleep(DataTransfer.SCAN_INTERVAL);
                    DataTransfer.log.info("ScanTask.queue.sleep: takes={}, interval={}, before.size={}, after.size={}", stopwatch.elapsed(), DataTransfer.SCAN_INTERVAL, beforeSize, queue.size());
                } catch (InterruptedException e) {
                    DataTransfer.log.error("ScanTask.queue.sleep failed", e);
                }
            }
        }

        public void scanFiles(Chan chan, Source src, List<Sink> sinks, File dir, ConcurrentLinkedQueue<FileItem> queue, AtomicInteger count) {
            File[] files = dir.listFiles();
            for (File f : files) {
                if (f.isFile()) {
                /*if (!chan.isTransfer(f.getName())) {
                    DataTransfer.log.info("file: {} false", f.getName());
                    continue;
                }*/
                    if (count.get() == DataTransfer.SCAN_FILE_COUNT) {
                        return;
                    }
                    queue.add(new FileItem(chan.getId(), src, sinks, f.getAbsolutePath()));
                    count.incrementAndGet();
                    //DataTransfer.log.info("add file: {{}} to queue.", f.getName());
                } else if (f.isDirectory()) {
                    scanFiles(chan, src, sinks, f, queue, count);
                }
            }
        }

    }


    public static class SendTask implements Runnable {
        int taskId;
        ConcurrentLinkedQueue<FileItem> queue;
        FileWriter fw;
        long lastTime;

        String currentDate;

        long nextDateMillis;

        public SendTask(int taskId, ConcurrentLinkedQueue<FileItem> queue) {
            this.taskId = taskId;
            this.queue = queue;
        }

        public String getCurrentDate() {
            if (System.currentTimeMillis() >= nextDateMillis) {
                SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
                Calendar cdar = Calendar.getInstance();
                currentDate = df.format(cdar.getTime());
                cdar.set(Calendar.HOUR_OF_DAY, 0);
                cdar.set(Calendar.MINUTE, 0);
                cdar.set(Calendar.SECOND, 0);
                cdar.set(Calendar.MILLISECOND, 0);
                cdar.add(Calendar.DATE, 1);
                nextDateMillis = cdar.getTimeInMillis();
            }
            return currentDate;
        }

        public FileWriter getFileWriter() {
            long currentTime = DataTransfer.minute_fromt(System.currentTimeMillis());
            if (fw == null) {
                try {
                    fw = new FileWriter(DataTransfer.PATH_TO_LOG + "/transfer_" + taskId + "_" + currentTime + ".temp", true);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                lastTime = currentTime;
                return fw;
            }
            if ((currentTime - lastTime) >= DataTransfer.LOG_SWAP_SECONDS) {
                try {
                    fw.flush();
                    fw.close();
                    File f = new File(DataTransfer.PATH_TO_LOG + "/transfer_" + taskId + "_" + lastTime + ".temp");
                    if (f.exists()) {
                        if (f.length() > 0) {
                            f.renameTo(new File(DataTransfer.PATH_TO_LOG + "/transfer_" + taskId + "_" + lastTime + ".log"));
                        } else {
                            f.delete();
                        }
                    }

                    fw = new FileWriter(DataTransfer.PATH_TO_LOG + "/transfer_" + taskId + "_" + currentTime + ".temp", true);
                    lastTime = currentTime;
                } catch (IOException e) {
                    log.error("getFileWriter failed", e);
                }
            }

            return fw;
        }

        private String getBucketName(File file, String chanId) {
            long time = file.lastModified();
            LocalDateTime now = LocalDateTime.now();
            long lowBound = LocalDateTime.of(now.getYear(), now.getMonth(), now.getDayOfMonth(), 0, 0).plusDays(-DataTransfer.DAYS).toEpochSecond(OffsetDateTime.now().getOffset()) * 1000;
            if (time < lowBound) {
                return null;
            }
            LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(time / 1000, (int) ((time % 1000) * 1000000), OffsetDateTime.now().getOffset());
            String bucketName = "chan-" + DataTransfer.HOST_IP + "-" + chanId + "-" + localDateTime.format(DateTimeFormatter.ofPattern("yyMMdd"));
            DataTransfer.createBucketIfNotExists(bucketName);
            return bucketName;
        }

        public int backupFile(File file, String bucketName, int prefixLength) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            String objectName = file.getAbsolutePath().substring(prefixLength);

            PutObjectArgs.Builder putObjectArgs = null;
            FileInputStream fis = null;
            ObjectWriteResponse resp = null;
            try {
                //执行上传
                fis = new FileInputStream(file);
                putObjectArgs = PutObjectArgs.builder().object(objectName)    //文件名
                        .bucket(bucketName) //存储目录名
                        .stream(fis, file.length(), -1);

                resp = DataTransfer.client.putObject(putObjectArgs.build());
                log.info("backupOssFile ok: takes=" + stopwatch.elapsed() + ", bucket=" + bucketName + ", len=" + file.length() + ", file=" + file.getAbsolutePath());
            } catch (FileNotFoundException e) {
                if (DataTransfer.checkObjectExists(bucketName, objectName)) {
                    log.info("backupOssFile repeat: takes=" + stopwatch.elapsed() + ", bucket=" + bucketName + ", len=" + file.length() + ", file=" + file.getAbsolutePath());
                    return 1;
                }
                log.warn("backupOssFile failed: takes=" + stopwatch.elapsed() + ", bucket=" + bucketName + ", len=" + file.length() + ", file=" + file.getAbsolutePath(), e);
            } catch (Exception e) {
                log.warn("backupOssFile failed: takes=" + stopwatch.elapsed() + ", bucket=" + bucketName + ", len=" + file.length() + ", file=" + file.getAbsolutePath(), e);
            } finally {
                if (fis != null) {
                    try {
                        fis.close();
                    } catch (IOException ex) {
                        log.warn("backupOssFile close failed: takes=" + stopwatch.elapsed() + ", bucket=" + bucketName + ", len=" + file.length() + ", file=" + file.getAbsolutePath(), ex);
                    }
                }
            }
            return 0;
        }

        public void logFileItem(long fileLen, long fileLastModified, String chanId, String sinkId, int status, String sinkIp, String parsePath, String subpath) {
//            if (fileLen == 0) {
//                return;
//            }
            FileWriter fw = getFileWriter();
            try {
                fw.write("(" + (fileLastModified / 1000) + "," + (System.currentTimeMillis() / 1000) + ",'" + DataTransfer.HOST_IP + "','" + chanId + "','" + sinkId + "'," + parsePath + ",'" + subpath // file.getName() //
                        + "'," + fileLen + "," + status + ",'" + sinkIp + "')\n");
                fw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void makeErrorFile(long fileLen, long fileLastModified, Sink sink, FileItem item, //
                                  int status, File srcFile, String subpath, String parsePath) {
            SinkDir sinkDir = sink.dirList.get(0);
            logFileItem(fileLen, fileLastModified, item.getChanId(), sink.id, status, sinkDir.ip, parsePath, subpath);

            if (!srcFile.exists()) {
                return;
            }
            String filePath = srcFile.getAbsolutePath();
            File err = new File(item.src.getErrDir(), getCurrentDate() + "/" + subpath);
            if (!err.getParentFile().exists()) {
                err.getParentFile().mkdirs();
            }
            if (err.exists()) {
                err.delete();
            }
            srcFile.renameTo(err);
            DataTransfer.log.warn("backupErrorFile ok: status={}, len={}, file={} ", status, fileLen, filePath);
        }

        @Override
        public void run() {

            while (true) {
                FileItem item = queue.poll();
                if (item == null) {
                    try {
                        getFileWriter();
                        Thread.sleep(DataTransfer.SEND_INTERVAL);
                        continue;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                //DataTransfer.log.info("starting process: {{}}", item.getFullName());
                File srcFile = new File(item.getFullName());
                if (!srcFile.exists()) {
                    continue;
                }
                //int prefixLength = new File(item.getSrc().getDir()).getAbsolutePath().length() + 1;
                int prefixLength = item.getSrc().getPrefixLength();
                String subpath = item.getFullName().substring(prefixLength);
                DataFileSenderBuilder.DataFileSender dfsender = DataFileSenderBuilder.getInstance(item.getSrc().getType(), subpath);
                if (dfsender == null) {
                    log.warn("不支持的数据类型: {}", item.getFullName());
                    continue;
                }

                //if needed, backup the file
                if (DataTransfer.BACKUP_ENABLE) {
                    String bucketName = getBucketName(srcFile, item.getChanId());
                    if (bucketName != null) {
                        int status = backupFile(srcFile, bucketName, prefixLength);
                        if (status == 1) {
                            continue;
                        }
                    }
                }

                boolean flag = false;
                int failCount = 0;
                Stopwatch stopwatch = Stopwatch.createStarted();
                long fileLen = srcFile.length();
                long fileLastModified = srcFile.lastModified();
                DataFileSenderBuilder.ValidateType validateType = null;
                for (Sink sink : item.sinks) {
                    //执行校验
                    if (sink.isValidate()) {
                        if (validateType == null) {
                            validateType = dfsender.validate(srcFile, subpath, item.getSrc().getSubpathNum());
                        }
                        if (validateType != DataFileSenderBuilder.ValidateType.FILE_OK) {
                            makeErrorFile(fileLen, fileLastModified, sink, item, //
                                    validateType.getCode(), srcFile, subpath, dfsender.parsePath(subpath));
                            continue;
                        }
                    }

                    int num = sink.getSinkDirCount();
                    long count = sink.count.incrementAndGet();
                    for (int i = 0; i < num; i++) {
                        int idx = (int) ((count + i) % num);
                        SinkDir sinkDir = sink.dirList.get(idx);

                        if (DataTransfer.QUOTA_ENABLE && srcFile.length() > sinkDir.getFree()) {
                            long used = FileUtil.getUsedByCmd(sinkDir.dir);
                            sinkDir.used.set(used);
                            if (srcFile.length() > sinkDir.getFree()) {
                                continue;
                            }
                        }
                        String destDir = sinkDir.getDir();
                        if (DataTransfer.SCP_ENABLE) {
                            flag = Ssh2Util.scp(item.src.ip, item.getFullName(), sinkDir.ip, destDir);
                        } else {
                            File dstFile = new File(destDir + "/" + item.getFullName().substring(prefixLength));
                            //DataTransfer.log.info("-------dd-----{}",dstFile.getAbsolutePath());
                            //flag = FileUtil.copyFileUsingStream(srcFile, dstFile);
                            flag = FileUtil.copyFileUsingStream(srcFile, dstFile, sinkDir.getTempDir());
                        }

                        if (flag) {
                            if (DataTransfer.QUOTA_ENABLE) {
                                long used = sinkDir.incrUsed(srcFile.length());
                                //System.out.println(sinkDir + "------ count: " + count + ", used: " + used);
                            }
                            logFileItem(fileLen, fileLastModified, item.getChanId(), sink.id, //
                                    DataFileSenderBuilder.ValidateType.FILE_OK.getCode(), //
                                    sinkDir.ip, dfsender.parsePath(subpath), subpath);
                            break;
                        }
                    }
                    if (!flag) {
                        failCount++;
                    }
                }

                if (failCount == 0) {
                    srcFile.delete();
                    DataTransfer.log.info("sendFile ok: takes={}, sinks={}, len={}, file={}", stopwatch.elapsed(), item.sinks.size(), fileLen, srcFile.getAbsolutePath());
                } else {
                    DataTransfer.log.error("sendFile fail: takes={}, sinks={}, len={}, file={}", stopwatch.elapsed(), item.sinks.size(), fileLen, srcFile.getAbsolutePath());
                }
            }
        }
    }

    public static class Chan {
        static final String BASE_MATCH_STR = ConfigUtil.getProperty("base.match.regex");
        String id;
        Source src;
        List<Sink> sinks = new ArrayList<>();
        boolean isBackup;
        String matchRegex;
        String noMatchRegex;

        public Chan(String id) {
            this.id = id;
        }

        public void setBackup(boolean backup) {
            isBackup = backup;
        }

        public String getId() {
            return id;
        }

        public Sink getSinkById(String sinkId) {
            for (Sink sink : sinks) {
                if (sink.id.contains(sinkId)) {
                    return sink;
                }
            }
            Sink sink = new Sink(sinkId);
            sinks.add(sink);
            return sink;
        }

        public Source getSrc() {
            return src;
        }

        public void setSrc(Source src) {
            this.src = src;
        }

        public List<Sink> getSinks() {
            return sinks;
        }

        public void setMatchRegex(String matchRegex) {
            this.matchRegex = matchRegex;
        }

        public void setNoMatchRegex(String noMatchRegex) {
            this.noMatchRegex = noMatchRegex;
        }

        public boolean isTransfer(String fileName) {
            boolean res1 = true;
            boolean res2 = false;

            if (!BASE_MATCH_STR.isEmpty()) {
                Pattern pb = Pattern.compile(BASE_MATCH_STR);
                Matcher mb = pb.matcher(fileName);
                if (!mb.matches()) {
                    return false;
                }
            }

            if (!matchRegex.isEmpty()) {
                Pattern p1 = Pattern.compile(matchRegex);
                Matcher m1 = p1.matcher(fileName);
                res1 = m1.matches();
            }

            if (!noMatchRegex.isEmpty()) {
                Pattern p2 = Pattern.compile(noMatchRegex);
                Matcher m2 = p2.matcher(fileName);
                res2 = m2.matches();
            }

            return res1 && !res2;
        }

        @Override
        public String toString() {
            return "Chan{" + "id='" + id + '\'' + ", src=" + src + ", sinks=" + sinks + ", isBackup=" + isBackup + ", matchRegex='" + matchRegex + '\'' + ", noMatchRegex='" + noMatchRegex + '\'' + '}';
        }
    }

    public static class FileItem {
        String chanId;
        Source src;
        List<Sink> sinks;
        String fullName;

        public FileItem(String chanId, Source src, List<Sink> sinks, String fullName) {
            this.chanId = chanId;
            this.src = src;
            this.sinks = sinks;
            this.fullName = fullName;
        }

        public String getChanId() {
            return chanId;
        }


        public Source getSrc() {
            return src;
        }

        public List<Sink> getSinks() {
            return sinks;
        }

        public String getFullName() {
            return fullName;
        }
    }

    public static class Source {
        String ip;
        String dir;

        String type;
        int subpathNum = -1;

        int prefixLength;

        File errDir;

        public Source(String ip, String dir) {
            this.ip = ip;
            this.dir = StringUtils.stripEnd(dir, "/");
            prefixLength = new File(dir).getAbsolutePath().length() + 1;
            errDir = new File(dir + "_error");
            errDir.mkdirs();
        }

        public int getSubpathNum() {
            return subpathNum;
        }

        public void setSubpathNum(int subpathNum) {
            this.subpathNum = subpathNum;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            if (StringUtils.isNotBlank(type)) {
                this.type = type.trim();
            }
        }

        public int getPrefixLength() {
            return prefixLength;
        }

        public String getIp() {
            return ip;
        }

        public String getDir() {
            return dir;
        }

        public File getErrDir() {
            return errDir;
        }

        @Override
        public String toString() {
            return "Source{" + "ip='" + ip + '\'' + ", dir='" + dir + '\'' + ", subpathNum=" + subpathNum + ", prefixLength=" + prefixLength + '}';
        }
    }

    public static class Sink {
        String id;
        List<SinkDir> dirList = new ArrayList<>();

        AtomicLong count = new AtomicLong(0);

        boolean validate = DataTransfer.VALIDATE_ENABLE;

        public Sink(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        public boolean isValidate() {
            return validate;
        }

        public void setValidate(boolean validate) {
            this.validate = validate;
        }

        public long incrCount() {
            return count.addAndGet(1);
        }

        public SinkDir getSinkDirById(String dirId) {
            for (SinkDir sinkDir : dirList) {
                if (sinkDir.id.equals(dirId)) {
                    return sinkDir;
                }
            }
            SinkDir sinkDir = new SinkDir(dirId);
            dirList.add(sinkDir);
            return sinkDir;
        }

        public int getSinkDirCount() {
            return dirList.size();
        }

        @Override
        public String toString() {
            return "Sink{" + "id='" + id + '\'' + ", validate=" + validate + ", dirList=" + dirList + '}';
        }
    }

    public static class SinkDir {
        String id;
        String ip = "127.0.0.1";
        String dir;
        long quota;
        AtomicLong used;
        File tmpDir;

        public SinkDir(String dirId) {
            this.id = dirId;
            this.used = new AtomicLong();
        }

        public long getFree() {
            return quota - used.get();
        }

        public void setQuota(long quota) {
            this.quota = quota;
        }

        public long incrUsed(long val) {
            return used.addAndGet(val);
        }

        public File getTempDir() {
            return tmpDir;
        }

        public String getDir() {
            return dir;
        }

        public void setDir(String dir) {
            this.dir = dir;
            this.tmpDir = new File(dir + "/tmp");
            if (!this.tmpDir.exists()) {
                this.tmpDir.mkdirs();
            } else {
                try {
                    FileUtils.cleanDirectory(tmpDir);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        @Override
        public String toString() {
            return "SinkDir{" + "id='" + id + '\'' + ", ip='" + ip + '\'' + ", dir='" + dir + '\'' + ", quota=" + quota + '}';
        }
    }

}