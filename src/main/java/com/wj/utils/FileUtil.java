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

    public static void writeFile(String path, String content) {
        File file =new File(path);
        File fileParent = file.getParentFile();
        try {
            if(!fileParent.exists()){
                fileParent.mkdirs();
            }
            file.createNewFile();
            FileWriter fileWriter =new FileWriter(file, true);
            content =content +System.getProperty("line.separator");
            fileWriter.write(content);
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) throws Exception {
        writeFile("d:\\event\\data.txt", "123");
    }
 }
