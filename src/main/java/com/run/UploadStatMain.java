package com.run;

import com.run.util.ConfigUtil;
import com.run.util.JdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class UploadStatMain {
    public static final Logger log = LoggerFactory.getLogger(UploadStatMain.class);
    private static final long MONITOR_PERIOD = ConfigUtil.getLong("monitor.capture.interval.minutes");
    private static final long TRANSFER_PERIOD = ConfigUtil.getLong("transfer.log.upload.interval.minutes");
    private static final String STAT_DIR = ConfigUtil.getProperty("transfer.log.path");
    private static final String preSQL = ConfigUtil.getProperty("transfer.stat.insert.sql", //
            "insert into transfer_log (receive_time, send_time,host_ip, channel_id, sink_id, " //
                    + "type_code,city_code,src_code,dataset,file_name, file_size, status, sink_ip) values ");
    private static final int preSQLColsNum = preSQL.split(",").length;

    public static void main(String[] args) {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        // 执行任务
        /*scheduledExecutorService.scheduleAtFixedRate(() -> {
            log.info("monitor start");
            Stopwatch stopwatch = Stopwatch.createStarted();
            batchInsertMonitorLog(Ssh2Util.getHostMetric());
            log.info("monitor end: {}", stopwatch.elapsed());
        }, 0, MONITOR_PERIOD, TimeUnit.MINUTES);*/

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            batchInsertTansferLog(STAT_DIR);
        }, 0, TRANSFER_PERIOD, TimeUnit.MINUTES);

//        while (true) {
//            try {
//                log.error("UploadStatMain awaitTermination: 30m");
//                scheduledExecutorService.awaitTermination(30, TimeUnit.MINUTES);
//            } catch (InterruptedException e) {
//                log.error("UploadStatMain awaitTermination error", e);
//            }
//        }
    }

    public static int batchInsertMonitorLog(List<String> records) {
        if (records.size() < 1) {
            return 0;
        }
        Connection conn = JdbcUtil.getConnection();
        int ret = 0;
        final String preSQL = "insert into monitor_log (monitor_time,host_ip,cpu_used,memery_used,disk_used) values ";
        StringBuilder sb = new StringBuilder();
        sb.append(preSQL);
        try {
            Statement stmt = conn.createStatement();
            for (int i = 0; i < records.size(); i++) {
                sb.append(records.get(i)).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            //System.out.println(sb.toString());
            ret = stmt.executeUpdate(sb.toString());
            log.info("insert table monitor_log, rows: {} successfully!", ret);
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public static int batchInsertTansferLog(String statDir) {
        File dir = new File(statDir);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new RuntimeException("日志目录错误:" + statDir);
        }
        File[] logs = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(".log") || (System.currentTimeMillis() - pathname.lastModified()) > TRANSFER_PERIOD * 60 * 2000;
            }
        });
        log.info("batchInsertTansferLog: statDir={}, count={}", statDir, logs.length);
        if (logs.length == 0) {
            return 0;
        }
        Connection conn = JdbcUtil.getConnection();
        int ret = 0;

        StringBuilder sb = new StringBuilder();
        sb.append(preSQL);
        String filePath = "";
        try {
            Statement stmt = conn.createStatement();
            int num = 0;
            for (int i = 0; i < logs.length; i++) {
                if (logs[i].length() == 0) {
                    logs[i].delete();
                    continue;
                }

                filePath = logs[i].getAbsolutePath();
                BufferedReader reader = new BufferedReader(new FileReader(logs[i]));
                String record = reader.readLine();
                while (record != null) {
                    if (record.split(",").length != preSQLColsNum) {
                        log.warn("batchInsertTansferLog skip row error: file={}, row={}", filePath, record);
                        record = reader.readLine();
                        continue;
                    }
                    sb.append(record).append(",");
                    if (num++ >= 3000) {
                        sb.deleteCharAt(sb.length() - 1);
                        ret = stmt.executeUpdate(sb.toString());
                        log.info("batchInsertTansferLog ok: file={}, rows={}", filePath, ret);
                        sb = new StringBuilder();
                        sb.append(preSQL);
                        num = 0;
                    }

                    record = reader.readLine();
                }

                if (num > 0) {
                    sb.deleteCharAt(sb.length() - 1);
                    ret = stmt.executeUpdate(sb.toString());
                    log.info("batchInsertTansferLog ok: file={}, rows={}", filePath, ret);
                    sb = new StringBuilder();
                    sb.append(preSQL);
                    num = 0;
                }
                reader.close();
                logs[i].delete();
            }

            stmt.close();
            conn.close();
        } catch (SQLException e) {
            //e.printStackTrace();
            log.warn("batchInsertTansferLog failed: file=" + filePath, e);
        } catch (FileNotFoundException e) {
            //e.printStackTrace();
            log.warn("batchInsertTansferLog failed: file=" + filePath, e);
        } catch (IOException e) {
            //e.printStackTrace();
            log.warn("batchInsertTansferLog failed: file=" + filePath, e);
        }
        return ret;
    }
}
