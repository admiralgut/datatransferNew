package com.run.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class FileUtil {
    public static Logger log = LoggerFactory.getLogger(FileUtil.class);
    public static final long ONE_TB = 1L << 40;
    public static final long ONE_GB = 1L << 30;
    public static final long ONE_MB = 1L << 20;

    public static boolean copyFileUsingStream(File source, File dest, File tmpDir) {
        File tmp = new File(tmpDir, source.getName());
        InputStream is = null;
        OutputStream os = null;
        try {
            is = new FileInputStream(source);
            os = new FileOutputStream(tmp);
            byte[] buffer = new byte[4096];
            int length;
            while ((length = is.read(buffer)) > 0) {
                os.write(buffer, 0, length);
            }
            os.close();
            os = null;
            File desDir = dest.getParentFile();
            if (!desDir.exists()) {
                desDir.mkdirs();
            }
            tmp.renameTo(dest);
            return true;
        } catch (FileNotFoundException e) {
            log.error("file not found: {}", source, e);
            return false;
        } catch (IOException e) {
            log.error("write file error: {}", dest, e);
            return false;
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    //throw new RuntimeException(e);
                    log.error("close file error: ", e);
                }
            }
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    //throw new RuntimeException(e);
                    log.error("close file error: ", e);
                }
            }
        }
    }

    public static boolean copyFileUsingStream(File source, File dest) {
        InputStream is = null;
        OutputStream os = null;
        try {
            File desDir = dest.getParentFile();
            if (!desDir.exists()) {
                desDir.mkdirs();
            }
            is = new FileInputStream(source);
            os = new FileOutputStream(dest);
            byte[] buffer = new byte[4096];
            int length;
            while ((length = is.read(buffer)) > 0) {
                os.write(buffer, 0, length);
            }
            // throw new FileNotFoundException(dest.getAbsolutePath());
            return true;
        } catch (FileNotFoundException e) {
            log.error("file not found: {}", source, e);
            return false;
        } catch (IOException e) {
            log.error("write file error: {}", dest, e);
            return false;
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    //throw new RuntimeException(e);
                    log.error("close file error: ", e);
                }
            }
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    //throw new RuntimeException(e);
                    log.error("close file error: ", e);
                }
            }
        }
    }

    public static long getUsedByCmd(String path) {
        String[] cmds = {"/bin/sh", "-c", "du " + path};
        long used = -1;
        try {
            Process pro = Runtime.getRuntime().exec(cmds);
            pro.waitFor();
            InputStream in = pro.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            int rowNum = 0;
            while ((line = reader.readLine()) != null) {
                rowNum++;
                if (rowNum == 1) {
                    used = Long.valueOf(line.split("\\s+")[0]) * 1024;
                }

            }
            pro.destroy();
            reader.close();
            in.close();
        } catch (InterruptedException e) {
            log.error("get used error", e);
        } catch (IOException e) {
            log.error("IO exception!", e);
        }

        return used;
    }

}
