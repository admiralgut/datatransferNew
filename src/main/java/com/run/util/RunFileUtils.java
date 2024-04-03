package com.run.util;

import com.google.common.base.Stopwatch;
import org.apache.commons.io.IOExceptionList;
import org.apache.commons.io.file.Counters;
import org.apache.commons.io.file.DeleteOption;
import org.apache.commons.io.file.DeletingPathVisitor;
import org.apache.commons.io.file.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liyanhong
 */
public class RunFileUtils {
    public static final DeleteOption[] EMPTY_DELETE_OPTION_ARRAY = new DeleteOption[0];
    private static final Logger log = LoggerFactory.getLogger(RunFileUtils.class);

    public static boolean deleteQuietly(File file) {
        if (file == null) {
            return false;
        } else {
            try {
                if (file.isDirectory()) {
                    cleanDirectory(file);
                }
            } catch (Exception e) {
            }

            try {
                return file.delete();
            } catch (Exception e) {
                return false;
            }
        }
    }

    public static void cleanDirectory(File directory) throws IOException {
        File[] files = verifiedListFiles(directory);
        List<Exception> causeList = new ArrayList();
        for (int i = 0; i < files.length; ++i) {
            File file = files[i];
            try {
                forceDelete(file);
            } catch (IOException e) {
                causeList.add(e);
            }
        }
        if (!causeList.isEmpty()) {
            throw new IOExceptionList(causeList);
        }
    }

    private static File[] verifiedListFiles(File directory) throws IOException {
        String message;
        if (!directory.exists()) {
            message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        } else if (!directory.isDirectory()) {
            message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        } else {
            File[] files = directory.listFiles();
            if (files == null) {
                throw new IOException("Failed to list contents of " + directory);
            } else {
                return files;
            }
        }
    }

    public static void forceDelete(File file) throws IOException {
        Counters.PathCounters deleteCounters;
        try {
            // deleteCounters = PathUtils.delete(file.toPath());
            deleteCounters = delete(file.toPath());
            log.info("forceDelete ok: dir=" + file.getAbsolutePath() + ", deleteCounters=" + deleteCounters);
        } catch (IOException e) {
            throw new IOException("Unable to delete file: " + file, e);
        }

        if (deleteCounters.getFileCounter().get() < 1L && deleteCounters.getDirectoryCounter().get() < 1L) {
            throw new FileNotFoundException("File does not exist: " + file);
        }
    }

    public static Counters.PathCounters delete(Path path) throws IOException {
        return delete(path, EMPTY_DELETE_OPTION_ARRAY);
    }

    public static Counters.PathCounters delete(Path path, DeleteOption... options) throws IOException {
        //return Files.isDirectory(path, new LinkOption[]{LinkOption.NOFOLLOW_LINKS}) ? deleteDirectory(path, options) : deleteFile(path, options);
        return Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) ? deleteDirectory(path, options) : PathUtils.deleteFile(path, options);
    }

    public static Counters.PathCounters deleteDirectory(Path directory, DeleteOption... options) throws IOException {
        // return ((DeletingPathVisitor)visitFileTree(new DeletingPathVisitor(Counters.longPathCounters(), options, new String[0]), (Path)directory)).getPathCounters();
        DeletingPathVisitor visitor = new MyDeletingPathVisitor(directory, Counters.longPathCounters(), options);
        Files.walkFileTree(directory, visitor);
        return visitor.getPathCounters();
    }

    private static class MyDeletingPathVisitor extends DeletingPathVisitor {

        private final Path directory;
        private final long interval = Long.parseLong(ConfigUtil.getProperty("data.retention.interval", "10000"));
        private final long sleepTime = Long.parseLong(ConfigUtil.getProperty("data.retention.sleep.time", "2000"));
        private long nextSleepTime = System.currentTimeMillis() + interval;
        private final Stopwatch stopwatch = Stopwatch.createStarted();

        public MyDeletingPathVisitor(Path directory, Counters.PathCounters pathCounter, DeleteOption[] deleteOption, String... skip) {
            super(pathCounter, deleteOption, skip);
            this.directory = directory;
        }

        public MyDeletingPathVisitor(Path directory, Counters.PathCounters pathCounter, String... skip) {
            super(pathCounter, skip);
            this.directory = directory;
        }

        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            FileVisitResult result = super.visitFile(file, attrs);
            Counters.PathCounters pathCounter = getPathCounters();
            if (System.currentTimeMillis() > nextSleepTime) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    log.error("visitFile sleep failed: " + file.toString(), e);
                }
                log.info("visitFile current counter: dir=" + directory + ", pathCounter=" + pathCounter);
                setNextSleepTime();
            }
            return result;
        }

        private void setNextSleepTime() {
            nextSleepTime = System.currentTimeMillis() + interval;
        }

    }

}
