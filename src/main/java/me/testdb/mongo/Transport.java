package me.testdb.mongo;

import com.mongodb.client.MongoCollection;
import me.testdb.service.InfluxDBService;
import me.testdb.service.MetaDataService;
import me.testdb.tool.Structure;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Transport {
    public static void main(String args[]) {
        MetaDataService metaDataService = new MetaDataService();
        MongoCollection mongoCollection = metaDataService.openCol();
        Long t1 = System.currentTimeMillis();
        String path = "F:\\DATASET\\Transport";

        File file = null;
        String fileFormat = null;
        FileInputStream in1;
        DataInputStream data_in;

        JSONObject jsonObject = new JSONObject();

        jsonObject.put("token","");
        jsonObject.put("auth",1);
        jsonObject.put("dataSet", "bgdata");
        jsonObject.put("subDataSet","Transport");

        jsonObject.put("description", "地铁数据集");
        jsonObject.put("type", 2);
        jsonObject.put("source", new JSONArray());
        jsonObject.put("target", "");
        JSONObject jsonObjectInType = new JSONObject();

        JSONObject jsonObjectTag = new JSONObject();
        JSONArray jsonArrayF = new JSONArray();
        JSONArray jsonArrayT = new JSONArray();

        StringBuilder result = new StringBuilder();
        try {
            file = new File(path);  // file path
            String[] filelist = file.list();
            int f = 0;
            for (int i = 0; i < filelist.length; i++) {
                Long ts = System.currentTimeMillis();
                Long systime = 0L;
                File readDir = new File(path + "/" + filelist[i]);
                String[] readDirlist = readDir.list();
                jsonObjectInType.put("state_" + String.valueOf(i), filelist[i]);
                if(f!=0) continue;
                f++;
                for (int j = 0; j < readDirlist.length; j++) {
                    File readfile = new File(path + "/" + filelist[i] + "/" + readDirlist[j]);
                    String[] fileinfo = readfile.getName().split("\\.");
                    String[] tagAndCol = fileinfo[0].split("_");
                    String tagName = tagAndCol[0];
                    String col = tagAndCol[1];
                    if (tagAndCol.length > 2) {
                        tagName = tagAndCol[0] + "_" + tagAndCol[1];
                        col = tagAndCol[2];
                    }
                    jsonArrayF.add(col + "_Double");
                    jsonArrayF.add(col + "_Float");
                    jsonArrayT.add("Double");
                    jsonArrayT.add("Double");
                    //System.out.println(tagName+"\n"+col);
                }
            }
            jsonObjectTag.put("state", jsonObjectInType);
            jsonObject.put("tag",jsonObjectTag);
            jsonObject.put("fieldName",jsonArrayF);
            jsonObject.put("fieldType",jsonArrayT);
            jsonObject.put("columns",jsonArrayF.size());
            jsonObject.put("rows", new JSONArray());
            jsonObject.put("dbType",1);
            jsonObject.put("dbName","bgdata");
            jsonObject.put("table","Transport");
            System.out.println(jsonObject.toString());
            metaDataService.saveMetaData(jsonObject.toString(),mongoCollection);

        } catch (
                Exception e) {

        }
    }
}

