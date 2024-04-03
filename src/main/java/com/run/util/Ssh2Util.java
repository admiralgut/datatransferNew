package com.run.util;

import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * java 远程执行linux命令
 */
public class Ssh2Util {
    public static Logger log = LoggerFactory.getLogger(Ssh2Util.class);
    private static String usr;
    private static String pwd;
    private static String monitorScriptPath;
    private static ThreadLocal<Map<String, Connection>> connectionMap;

    static {
        usr = ConfigUtil.getProperty("ssh2.username");
        pwd = ConfigUtil.getProperty("ssh2.password");
        monitorScriptPath = ConfigUtil.getProperty("monitor.script.path");
        connectionMap = new ThreadLocal<>();
    }

    public static void main(String[] args) throws IOException {
        //String remoteCommand = "sh /datatransfer/stop.sh";
        //String remoteCommand = "scp /dbc/ddd.cfg root@192.168.15.96:/dbc";
        //String localCommand = "sh /home/hadoop/tmp/model.sh";
        //RemoteSubmitCommand("192.168.15.91", "echo1 888 || echo 999");
        // boolean r = uploadFile("192.168.15.91", "files/db.sql", "/dbc");
        //boolean r = downloadFile("192.168.15.91", "/dbc/dbc91.txt", "files/");
        /*if (r) {
            System.out.println(666);
        }*/

        //scp("192.168.15.91", "/dbc/dbc91.txt", "192.168.15.96", "/dbc/ddd88");

        // execCommand("192.168.15.96", "mkdir -p  /dbc/ddd5");
        // execCommand("192.168.15.96", "sh /home/dbc/host-monitor.sh");

        //System.out.println(getHostMetric());
//        String ip = "192.168.13.20";
//        execCommand(ip, 22, "mkdir -p /home/dbc/dddddd88");

        for (String s : getHostMetric()) {
            System.out.println(s);
        }


    }

    /**
     * 远程执行 linux 命令
     */

    public static Connection getConnection(String ip) {
        Connection conn;
        Map<String, Connection> connMap = connectionMap.get();
        if (connMap == null) {
            connMap = new HashMap<>();
            connectionMap.set(connMap);
        } else {
            conn = connMap.get(ip);
            if (conn != null) {
                if (conn.isAuthenticationComplete()) {
                    return conn;
                } else {
                    connMap.remove(conn);
                }

            }
        }
        conn = new Connection(ip);
        try {
            conn.connect();
            boolean isAuthed = conn.authenticateWithPassword(usr, pwd);
            if (!isAuthed) {
                log.error("Authentication failed, user: {}", usr);
                throw new IOException("Authentication failed.");
            }
            connMap.put(ip, conn);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void closeConnection(String ip) {
        Connection conn;
        Map<String, Connection> connMap = connectionMap.get();
        if (connMap != null) {
            conn = connMap.get(ip);
            if (conn != null) {
                conn.close();
                connMap.remove(ip);
            }
        }
    }


    public static CmdRes execCommand(String ip, int port, String cmd) {
        CmdRes ret;
        Connection conn = new Connection(ip, port);
        Session session;
        StringBuilder sbStdOut = new StringBuilder();
        StringBuilder sbStdErr = new StringBuilder();
        try {
            conn.connect();
            boolean isAuthed = conn.authenticateWithPassword(usr, pwd);
            if (!isAuthed) {
                log.error("Authentication failed, user: {}", usr);
                throw new IOException("Authentication failed.");
            }
            session = conn.openSession();
            session.execCommand(cmd);

            InputStream stdOut = session.getStdout();
            InputStream stdErr = session.getStderr();
            byte[] buffer = new byte[1024];
            while (true) {
                if ((stdOut.available() == 0) && (stdErr.available() == 0)) {
                    int conditions = session.waitForCondition(ChannelCondition.STDOUT_DATA | ChannelCondition.STDERR_DATA
                            | ChannelCondition.EOF, 20000);


                    if ((conditions & ChannelCondition.TIMEOUT) != 0) {
                        log.error("Timeout while waiting for data from peer.");
                        return new CmdRes(false, "Timeout");
                    }

                    if ((conditions & ChannelCondition.EOF) != 0) {
                        if ((conditions & (ChannelCondition.STDOUT_DATA | ChannelCondition.STDERR_DATA)) == 0) {
                            log.info("break: {}", conditions);
                            break;
                        }
                    }
                }

                while (stdOut.available() > 0) {
                    int len = stdOut.read(buffer);
                    if (len > 0) {
                        sbStdOut.append(new String(buffer, 0, len));
                    }
                }

                while (stdErr.available() > 0) {
                    int len = stdErr.read(buffer);
                    if (len > 0) {
                        sbStdErr.append(new String(buffer, 0, len));
                    }
                }
            }

            log.info("out status: {}, std[{}]: {}, err[{}]:{}", session.getExitStatus(), sbStdOut.length(), sbStdOut, sbStdErr.length(), sbStdErr);

            if (session.getExitStatus() == 0 || (sbStdOut.length() > 0 && sbStdErr.length() == 0)) {
                log.info("exec command: {{}} on [{}] success, status: {{}} {}", cmd, ip, session.getExitStatus(), sbStdOut);
                ret = new CmdRes(true, sbStdOut.toString());
            } else {
                log.error("exec command error: {}, status:{}", sbStdErr, session.getExitStatus());
                ret = new CmdRes(false, sbStdErr.toString());
            }

            stdOut.close();
            stdErr.close();
            session.close();
            conn.close();
            return ret;
        } catch (IOException e) {
            log.error("Exec: {}  error: {} ", cmd, e);
            return new CmdRes(false, e.getMessage());
            //throw new RuntimeException("Exec shell error");
        }
    }

    public static void mkdir(String ip, int port, String path) {
        execCommand(ip, port, "mkdir -p " + path);
    }

    public static boolean uploadFile(String ip, String srcFile, String destDir) {
        boolean status = false;
        Connection conn = new Connection(ip);
        try {
            conn.connect();
            boolean isAuthed = conn.authenticateWithPassword(usr, pwd);
            if (isAuthed) {
                log.info("login successfully!");
            } else {
                log.error("login failed!");
                return false;
            }
            SCPClient scpClient = conn.createSCPClient();
            scpClient.put(srcFile, destDir);
            status = true;
        } catch (IOException e) {
            log.error("upload file error: ", e);
            return false;
        } finally {
            conn.close();
        }
        return status;
    }


    public static boolean downloadFile(String ip, String srcFile, String destDir) {
        boolean status = false;
        Connection conn = new Connection(ip);
        try {
            conn.connect();
            boolean isAuthed = conn.authenticateWithPassword(usr, pwd);
            if (isAuthed) {
                log.info("login successfully!");
            } else {
                log.error("login failed!");
                return false;
            }
            SCPClient scpClient = conn.createSCPClient();
            scpClient.get(srcFile, destDir);
            status = true;
        } catch (IOException e) {
            log.error("download file error: ", e);
            return false;
        } finally {
            conn.close();
        }
        return status;
    }

    public static boolean scp(String srcIp, String srcFile, String destIp, String destDir) {
        String cmd = String.format("ssh root@%s \"mkdir -p %s\" && scp %s root@%s:%s", destIp, destDir, srcFile, destIp, destDir);
        CmdRes execStatus = execCommand(srcIp, 6022, cmd);
        if (execStatus.status) {
            log.info("Exec command: {{}} successfully!", cmd);
        } else {
            log.info("failed to Exec command: {{}}!", cmd);
        }
        return execStatus.status;
    }

    public static boolean submitModelDataSync(String command) throws IOException {
        boolean flag = true;
        try {
            final Process process = Runtime.getRuntime().exec(command);
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            long startTime = System.currentTimeMillis() / 1000;
            while (true) {
                long endTime = System.currentTimeMillis() / 1000;
                if (endTime - startTime > 10000000) {
                    return false;
                }
                String line = br.readLine();
                if (line.contains("success")) {
                    flag = true;
                    break;
                }
                if (line.contains("Error") || line.contains("error")) {
                    flag = false;
                    break;
                }
            }
            br.close();
        } catch (Exception e) {
            log.error("local exec shell error: ", e);
        }
        return flag;
    }

    public static List<String> getHostMetric() {
        List<String> lines = new ArrayList<>();
        for (String hostIpPort : ConfigUtil.getMonitorHosts()) {
            String[] strArr = hostIpPort.split(":");
            CmdRes res = execCommand(strArr[0], Integer.valueOf(strArr[1]), "sh " + monitorScriptPath);
            if (res.status == true) {
                lines.add(res.stdOut);
            } else {
                log.error("exec command error: {}, status:{}", res.stdOut, res.status);
            }
        }
        return lines;
    }

    static class CmdRes {
        boolean status;
        String stdOut;

        public CmdRes(boolean status, String stdOut) {
            this.status = status;
            this.stdOut = stdOut;
        }
    }

}

