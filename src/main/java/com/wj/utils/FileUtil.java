package com.wj.utils;

import java.io.*;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/
public class FileUtil {

    public static String readJson(String path) throws Exception {
        File file = new File(path);
        InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line = "";
        StringBuffer sb = new StringBuffer();
        while ((line = bufferedReader.readLine()) != null) {
            sb.append(line);
        }
        return sb.toString();
    }
 }
