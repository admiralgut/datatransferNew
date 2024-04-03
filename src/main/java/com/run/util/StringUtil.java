/**
 * Copyright 2014, 2016 Red Hat Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.run.util;

import java.io.File;


public class StringUtil {

    public static String rstrip(String s) {
        char[] val = s.toCharArray();

        int start = 0;
        int length = val.length;

        while ((start < length) && (Character.isWhitespace(val[length - 1])))
            length--;

        return s.substring(start, length);
    }


    public static String strip(String s) {
        char[] val = s.toCharArray();

        int start = 0;
        int length = val.length;

        while ((start < length) && (Character.isWhitespace(val[start])))
            start++;
        while ((start < length) && (Character.isWhitespace(val[length - 1])))
            length--;

        return s.substring(start, length);
    }

    public static void addLog2Sb(StringBuilder sb, File file, String chanId, String sinkId, int status) {
        sb.append("(").append(file.lastModified() / 1000).append(",")
                .append(System.currentTimeMillis() / 1000).append(",'")
                .append(chanId).append("','")
                .append(sinkId).append("','")
                .append(file.getAbsolutePath()).append("',")
                .append(file.length()).append(",")
                .append(status).append(")\n");
    }

    public static String parsePath(int preLen, String pathStr) {
        String stdPath = pathStr.substring(preLen);
        String[] pa = stdPath.split("\\\\|/", 0);
        return "'" + pa[0] + "','" + pa[1] + "','" + pa[2] + "','" + pa[3] + "'";
    }

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

    public static String parseCheckedPath(String subpath) {
        String[] pa = subpath.split("\\\\|/", 0);
        // 尽可能匹配
        StringBuilder sb = new StringBuilder();
        String type;
        if (pa.length > 0 && "115".equals(pa[0])) {
            sb.append("'").append(pa[0]).append("','");
        } else {
            sb.append("'NA','");
        }
        if (pa.length > 1 && isNumber(pa[1], 6)) {
            sb.append(pa[1]).append("','");
        } else {
            sb.append("NA','");
        }
        if (pa.length > 2 && isNumber(pa[2], 7)) {
            sb.append(pa[2]).append("','");
        } else {
            sb.append("NA','");
        }
        if (pa.length > 3 && pa[3].startsWith("WA_")) {
            sb.append(pa[3]).append("'");
        } else {
            sb.append("NA'");
        }
        return sb.toString();
    }

}
