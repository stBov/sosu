package com.yztsoft.sosu.utils;

import java.io.*;

/**
 * @classname: FileUtil
 * @description:
 * @author: Shi Shijie
 * @create: 2018-11-22 14:35
 **/

public class FileUtil {

    public static String readJsonDefn(String url)  {
        StringBuffer bufferJSON = new StringBuffer();
        InputStream input;
        try {
            input = ClassLoaderUtil.getResourceAsStream(url,FileUtil.class);
            DataInputStream inputStream = new DataInputStream(input);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = br.readLine()) != null) {
                bufferJSON.append(line);
            }
            inputStream.close();
            br.close();
            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }catch(IOException e){
            e.printStackTrace();
            return null;
        }
        return bufferJSON.toString();
    }

    public static void main(String[] args){
        String filename="config/mappings/hotel.json";
        String content = readJsonDefn(filename);
        System.out.println(content);
    }

}
