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

import com.run.exceptions.IniParserException;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Simple read/write main class, mostly for testing purposes
 */
public class Main {
    public static void main(String[] args) throws IniParserException, IOException {
        //new Ini().read(Paths.get(args[0])).write(Paths.get(args[1]));
        Ini ini = new Ini().read(Paths.get("files/test.ini"));
        Map<String, Map<String, String>> secs = ini.getSections();
        for (Map.Entry<String, Map<String, String>> entry : secs.entrySet()) {
            int num = (56 - 2 - entry.getKey().length()) / 2;

            System.out.println(StringUtils.center(entry.getKey(), 56, '='));
            for (Map.Entry<String, String> en : entry.getValue().entrySet()) {
                System.out.println("key: " + en.getKey() + ", val: " + en.getValue());
            }
            System.out.println("========================================================");
        }
        ini.write(Paths.get("files/test1.ini"));
    }
}
