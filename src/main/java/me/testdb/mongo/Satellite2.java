package me.testdb.mongo;

import com.mongodb.client.MongoCollection;
import com.squareup.moshi.Json;
import me.testdb.service.InfluxDBService;
import me.testdb.service.MetaDataService;
import me.testdb.tool.Pair;
import me.testdb.tool.Structure;
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

public class Satellite2 {
    public static void main(String args[])
    {
        MetaDataService metaDataService=new MetaDataService();
        MongoCollection mongoCollection=metaDataService.openCol();
        Long t1=System.currentTimeMillis();
        String path="F:\\DATASET\\Satellite\\桌面联试试验数据";
        //path=args[0];
        File file = null;
        String fileFormat = null;
        FileInputStream in1;
        DataInputStream data_in;

        StringBuilder result=new StringBuilder();
        try {
            file = new File(path);  // file path
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
                Long ts=System.currentTimeMillis();
                Long systime = 0L;
                File readfile = new File(path + "/" + filelist[i]);
                //System.out.println(path + "/" + filelist[i]);
                String[] fileinfo = readfile.getName().split("\\.");
                //System.out.println("Getfile:"+readfile.getName());
                fileFormat = fileinfo[fileinfo.length - 1];
                if (fileFormat.equals("xlsx")) continue;
                //System.out.println("file format:" + fileFormat);
                String tableName = fileinfo[0].split("_")[0];
                //System.out.println("taglName:" + tableName);
                String whatIs = fileinfo[0].split("_")[1];
                if (whatIs.equals("table")) continue;


                JSONObject jsonObject = new JSONObject();

                jsonObject.put("token","");
                jsonObject.put("auth",1);
                jsonObject.put("dataSet", "Satellite");
                jsonObject.put("description", "卫星数据集");
                jsonObject.put("type", 2);
                jsonObject.put("source", new JSONArray());
                jsonObject.put("target", "");
                jsonObject.put("tag",new JSONObject());
                /*JSONObject jsonObjectInType = new JSONObject();
                jsonObjectInType.put("state_1", "Optional");
                jsonObjectInType.put("state_2", "Protocol");
                JSONObject jsonObjectTag = new JSONObject();
                jsonObjectTag.put("type", jsonObjectInType);
                JSONArray jsonArrayField=new JSONArray();
                JSONArray jsonArrayType= new JSONArray();*/


                Map<String, String> Tag = new HashMap<String, String>();
                Tag.put("tag", tableName);
                File readTable = new File(path + "/" + tableName + "_table.txt");
                List<String> colName = new ArrayList<String>();
                List<Pair<String, String>> struc = new ArrayList<Pair<String, String>>();
                BufferedReader br;
                try {
                    jsonObject.put("subDataSet", tableName);
                    JSONArray jsonArrayF=new JSONArray();
                    JSONArray jsonArrayT=new JSONArray();
                    br = new BufferedReader(new FileReader(readTable));
                    String line = "";
                    while ((line = br.readLine()) != null && line != "") {
                        Pair<String, String> p = new Pair<String, String>();
                        p.setFirst(line);
                        p.setSecond("Double");

                        jsonArrayF.add(line);
                        jsonArrayT.add("Double");
                        struc.add(p);
                    }

                    jsonObject.put("fieldName",jsonArrayF);
                    jsonObject.put("fieldType",jsonArrayT);


                    //struc.add(new Pair<String, String>().make_pair(tableName, "Tag"));
                    br.close();

                    Structure structure = new Structure();
                    structure.setHasHeader(-1);
                    structure.setStartLine(1);
                    structure.setHasTime(false);
                    structure.setFileStruct(struc);
                    FileInputStream in = new FileInputStream(readfile);
                    br = new BufferedReader(new InputStreamReader(in,"gbk"));
                    //br = new BufferedReader(new FileReader(readfile));
                    line = "";
                    while ((line = br.readLine()) != null && line != "") {
                        systime++;
                        Map<String, Double> fd = new HashMap<String, Double>();
                        String[] data = line.split(",");
                        for (int k = 0; k < data.length; k++) {
                            Double t = Double.valueOf(data[k]);
                            fd.put(struc.get(k).getFirst(), t);
                        }

                    }
                    jsonObject.put("columns",jsonArrayF.size());
                    jsonObject.put("rows", new JSONArray());
                    jsonObject.put("dbType",1);
                    jsonObject.put("dbName","Satellite");
                    jsonObject.put("table",jsonObject.getString("subDataSet"));
                    System.out.println(jsonObject.toString());
                    metaDataService.saveMetaData(jsonObject.toString(),mongoCollection);

                } catch (Exception e) {

                }
            }
        } catch (Exception e) {

        }

    }
}
