package com.run.util;


import net.sf.sevenzipjbinding.IInArchive;
import net.sf.sevenzipjbinding.SevenZip;
import net.sf.sevenzipjbinding.impl.RandomAccessFileInStream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.zip.ZipFile;


/**
 * 文件解压缩工具类
 */
public class RarZip7zUtiles {
    private static final Logger log = LoggerFactory.getLogger(RarZip7zUtiles.class);

    public static boolean validateRar(File file, String passWord) {
        boolean ret = false;
        RandomAccessFile randomAccessFile = null;
        IInArchive inArchive = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            if (StringUtils.isNotBlank(passWord)) {
                inArchive = SevenZip.openInArchive(null, new RandomAccessFileInStream(randomAccessFile), passWord);
            } else {
                inArchive = SevenZip.openInArchive(null, new RandomAccessFileInStream(randomAccessFile));
            }

            ret = true;
        } catch (Exception e) {
            log.error("validateRar error", e);
            return false;
        } finally {
            try {
                if (inArchive != null) {
                    inArchive.close();
                }
                randomAccessFile.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return ret;
    }

    public static boolean validateZip(File zipFile) {
        ZipFile zip = null;
        boolean ret = false;
        try {
            zip = new ZipFile(zipFile, Charset.forName("utf-8"));
            ret = true;
        } catch (Exception e) {
            if (e.getMessage().equals("invalid CEN header (encrypted entry)")) {
                ret = true;
            } else {
                log.warn("validateZip failed: file={}, msg={}", zipFile.getAbsolutePath(), e.getMessage());
            }
            // e.printStackTrace();
        } finally {
            if (zip != null) {
                try {
                    zip.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return ret;
    }

    public static boolean validateZip1(File zipFile) {
        ZipFile zip = null;
        boolean ret = false;
        try {
            zip = new ZipFile(zipFile, Charset.forName("utf-8"));
            ret = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (zip != null) {
                try {
                    zip.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return ret;
    }

    public static void main(String[] args) {
        String filePath = args[0];
        File file = new File(filePath);
        if (filePath.endsWith(".rar")) {
            boolean result = validateRar(file, null);
            System.out.println("validateRar: " + file.getAbsolutePath() + " == > " + result);
        } else {
            boolean result = validateZip(file);
            System.out.println("validateZip: " + file.getAbsolutePath() + " == > " + result);
        }

    }
}