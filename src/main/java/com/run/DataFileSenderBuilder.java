package com.run;

import com.google.common.collect.Maps;
import com.run.util.RarZip7zUtiles;

import java.io.File;
import java.util.Map;

/**
 * @author liyanhong
 */
public class DataFileSenderBuilder {

    public static final String PATH_SEP = "/|\\\\";

    private static final Map<String, DataFileSender> dfSenderMap = Maps.newHashMap();

    static {
        dfSenderMap.put("115", new Data115FileSender());
        dfSenderMap.put("diaozheng", new DataDiaozhengFileSender());
    }

    public static DataFileSender getInstance(String type, String subpath) {
        DataFileSender dfs;
        // String dataType = StringUtils.substringBefore(subpath, PATH_SEP);
        if (type != null) {
            dfs = dfSenderMap.get(type);
            if (dfs == null) {
                synchronized (dfSenderMap) {
                    dfs = dfSenderMap.get(type);
                    if (dfs == null) {
                        dfs = new DataSimpleFileSender(type);
                        dfSenderMap.put(type, dfs);
                    }
                }
            }
            return dfs;
        }
        String[] pa = subpath.split(PATH_SEP, 0);
        dfs = dfSenderMap.get(pa[0]);
        if (dfs == null) {
            dfs = dfSenderMap.get(pa[1]);
        }
        return dfs;
    }

    public enum ValidateType {
        FILE_OK(1),//
        POSTFIX_ERROR(2),//
        FORMAT_ERROR(3),//
        SUBPATH_ERROR(4);

        private final int code;

        ValidateType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

    }

    public static abstract class DataFileSender {

        public abstract String parsePath(String subpath);
        public static boolean isNumber(String value, int len) {
            if (value == null) {
                return false;
            } else {
                int sz = value.length();
                if (sz != len) {
                    return false;
                }
                for (int i = 0; i < sz; ++i) {
                    if (!Character.isDigit(value.charAt(i))) {
                        return false;
                    }
                }
                return true;
            }
        }
        public ValidateType validate(File srcFile, String subpath, int subpathNum) {
            if (!(subpath.endsWith(".zip") || subpath.endsWith(".rar"))) {
                return ValidateType.POSTFIX_ERROR;
            }
            if (subpath.endsWith(".zip") && !RarZip7zUtiles.validateZip(srcFile)) {
                return ValidateType.FORMAT_ERROR;
            } else if (subpath.endsWith(".rar") && !RarZip7zUtiles.validateRar(srcFile, null)) {
                return ValidateType.FORMAT_ERROR;
            }
            if (subpathNum > 0 && subpath.split(PATH_SEP, 0).length != subpathNum) {
                return ValidateType.SUBPATH_ERROR;
            }
            return ValidateType.FILE_OK;
        }

    }

    public static class Data115FileSender extends DataFileSender {
        @Override
        public String parsePath(String subpath) {
            // 尽可能匹配
            String[] pa = subpath.split(PATH_SEP, 0);
            StringBuilder sb = new StringBuilder();

            // 维度1：数据类型
            if (pa.length > 0 && "115".equals(pa[1])) {
                sb.append("'").append(pa[1]).append("','");
            } else {
                sb.append("'NA','");
            }

            // 维度2：地市编码
            if (pa.length > 1 && isNumber(pa[0], 6)) {
                sb.append(pa[0]).append("','");
            } else {
                sb.append("NA','");
            }

            // 维度3：网站编码
            if (pa.length > 2 && isNumber(pa[2], 7)) {
                sb.append(pa[2]).append("','");
            } else {
                sb.append("NA','");
            }

            // 维度4：数据集
            if (pa.length > 3 && pa[3].startsWith("WA_")) {
                sb.append(pa[3]).append("'");
            } else {
                sb.append("NA'");
            }
            return sb.toString();
        }
    }

    public static class DataDiaozhengFileSender extends DataFileSender {
        @Override
        public String parsePath(String subpath) {
            // 尽可能匹配
            String[] pa = subpath.split(PATH_SEP, 0);
            StringBuilder sb = new StringBuilder();

            // 维度1：数据类型
            sb.append("'").append(pa[0]).append("','");

            // 维度2：地市编码
            if (pa.length > 1) {
                sb.append(pa[1]).append("','");
            } else {
                sb.append("NA','");
            }

            // 维度3：网站编码
            if (pa.length > 2) {
                sb.append(pa[2]).append("','");
            } else {
                sb.append("NA','");
            }

            // 维度4：数据集
            if (pa.length > 3) {
                sb.append(pa[3]).append("'");
            } else {
                sb.append("NA'");
            }
            return sb.toString();
        }

    }

    public static class DataSimpleFileSender extends DataFileSender {

        private final String type;

        public DataSimpleFileSender(String type) {
            super();
            this.type = type;
        }

        @Override
        public String parsePath(String subpath) {
            // 尽可能匹配
            String[] pa = subpath.split(PATH_SEP, 0);
            StringBuilder sb = new StringBuilder();

            // 维度1：数据类型
            sb.append("'").append(type).append("','");

            // 维度2：地市编码
            sb.append("NA','");

            // 维度3：网站编码
            sb.append("NA','");

            // 维度4：数据集
            sb.append("NA'");

            return sb.toString();
        }

    }

}
