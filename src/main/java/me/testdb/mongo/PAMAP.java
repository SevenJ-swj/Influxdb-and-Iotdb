package me.testdb.mongo;

import com.mongodb.client.MongoCollection;
import me.testdb.service.InfluxDBService;
import me.testdb.service.MetaDataService;
import me.testdb.tool.Pair;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PAMAP {
    public static List<Pair<String, String>> getHead() {
        List<Pair<String, String>> struc = new ArrayList<Pair<String, String>>();
        struc.add(new Pair<String, String>().make_pair("time", "time"));
        struc.add(new Pair<String, String>().make_pair("activityID", "Long"));
        struc.add(new Pair<String, String>().make_pair("heartRate", "Double"));
        String[] pre = new String[3];
        pre[0] = "hand";
        pre[1] = "chest";
        pre[2] = "shoe";
        for (int i = 0; i < pre.length; i++) {
            struc.add(new Pair<String, String>().make_pair(pre[i] + "_temperature", "Double"));
            for (int j = 0; j < 3; j++)
                struc.add(new Pair<String, String>().make_pair(pre[i] + "_accereration" + String.valueOf(j), "Double"));
            for (int j = 0; j < 3; j++)
                struc.add(new Pair<String, String>().make_pair(pre[i] + "_gyroscope" + String.valueOf(j), "Double"));
            for (int j = 0; j < 3; j++)
                struc.add(new Pair<String, String>().make_pair(pre[i] + "_magnetometer" + String.valueOf(j), "Double"));
            for (int j = 0; j < 4; j++)
                struc.add(new Pair<String, String>().make_pair(pre[i] + "_orientation" + String.valueOf(j), "Double"));
        }
        return struc;
    }

    public static void main(String args[]) {

        MetaDataService metaDataService=new MetaDataService();
        MongoCollection mongoCollection=metaDataService.openCol();
        Long t1 = System.currentTimeMillis();
        String path = "F:\\DATASET\\PAMAP\\PAMAP_Dataset";
        File file = null;
        String fileFormat = null;
        FileInputStream in1;
        DataInputStream data_in;


        StringBuilder result = new StringBuilder();
        List<Pair<String, String>> struc = getHead();


        JSONObject jsonObject = new JSONObject();
        jsonObject.put("token","");
        jsonObject.put("auth",1);
        jsonObject.put("dataSet", "bgdata");
        jsonObject.put("subDataSet", "PAMAP1");

        jsonObject.put("description", "运动采集数据");
        jsonObject.put("type", 2);
        jsonObject.put("source", new JSONArray());
        jsonObject.put("target", "");

        JSONObject jsonObjectInType = new JSONObject();
        jsonObjectInType.put("state_1", "Indoor");
        jsonObjectInType.put("state_2", "Outdoor");
        JSONObject jsonObjectTag = new JSONObject();
        jsonObjectTag.put("tag", jsonObjectInType);
        JSONArray jsonArrayField=new JSONArray();
        JSONArray jsonArrayType= new JSONArray();
        for(int i=1;i<struc.size();i++)
        {
            String fieldN=struc.get(i).getFirst();
            String ft=struc.get(i).getSecond();
            jsonArrayField.add(fieldN);
            jsonArrayType.add(ft);
        }
        jsonObject.put("tag",jsonObjectTag);
        jsonObject.put("fieldName",jsonArrayField);
        jsonObject.put("fieldType",jsonArrayType);
        jsonObject.put("columns",struc.size()-1);
        jsonObject.put("rows", new JSONArray());
        jsonObject.put("dbType",1);
        jsonObject.put("dbName","bgdata");
        jsonObject.put("table","PAMAP1");
        System.out.println(jsonObject.toString());
        metaDataService.saveMetaData(jsonObject.toString(),mongoCollection);

    }
}
