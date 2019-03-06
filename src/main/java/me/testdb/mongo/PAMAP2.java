package me.testdb.mongo;

import com.mongodb.client.MongoCollection;
import me.testdb.service.MetaDataService;
import me.testdb.tool.Pair;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

public class PAMAP2 {
    public static List<Pair<String, String>> getHead() {

        List<Pair<String,String>> struc=new ArrayList<Pair<String, String>>();
        struc.add(new Pair<String,String>().make_pair("time","time"));
        struc.add(new Pair<String,String>().make_pair("activityID","Long"));
        struc.add(new Pair<String,String>().make_pair("heartRate","Double"));
        String[] pre=new String[3];
        pre[0]="hand";
        pre[1]="chest";
        pre[2]="ankle";
        for(int i=0;i<pre.length;i++){
            struc.add(new Pair<String,String>().make_pair(pre[i]+"_temperature","Double"));
            for(int j=0;j<3;j++)
                struc.add(new Pair<String,String>().make_pair(pre[i]+"_accereration_16scale"+String.valueOf(j),"Double"));
            for(int j=0;j<3;j++)
                struc.add(new Pair<String,String>().make_pair(pre[i]+"_accereration_6scale"+String.valueOf(j),"Double"));
            for(int j=0;j<3;j++)
                struc.add(new Pair<String,String>().make_pair(pre[i]+"_gyroscope"+String.valueOf(j),"Double"));
            for(int j=0;j<3;j++)
                struc.add(new Pair<String,String>().make_pair(pre[i]+"_magnetometer"+String.valueOf(j),"Double"));
            for(int j=0;j<4;j++)
                struc.add(new Pair<String,String>().make_pair(pre[i]+"_orientation"+String.valueOf(j),"Double"));
        }
        return struc;
    }

    public static void main(String args[]) {
        Long t1 = System.currentTimeMillis();
        String path = "F:\\DATASET\\PAMAP\\PAMAP2_Dataset";
        File file = null;
        String fileFormat = null;
        FileInputStream in1;
        DataInputStream data_in;

        MetaDataService metaDataService=new MetaDataService();
        MongoCollection mongoCollection=metaDataService.openCol();

        StringBuilder result = new StringBuilder();
        List<Pair<String, String>> struc = getHead();


        JSONObject jsonObject = new JSONObject();
        jsonObject.put("token","");
        jsonObject.put("auth",1);
        jsonObject.put("dataSet", "bgdata");
        jsonObject.put("subDataSet", "PAMAP2");

        jsonObject.put("description", "运动采集数据");
        jsonObject.put("type", 2);
        jsonObject.put("source", new JSONArray());
        jsonObject.put("target", "");

        JSONObject jsonObjectInType = new JSONObject();
        jsonObjectInType.put("state_1", "Optional");
        jsonObjectInType.put("state_2", "Protocol");
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
        jsonObject.put("columns",Long.valueOf(struc.size()-1));
        jsonObject.put("rows",new JSONArray());
        jsonObject.put("dbType",1);
        jsonObject.put("dbName","bgdata");
        jsonObject.put("table","PAMAP2");
        metaDataService.saveMetaData(jsonObject.toString(),mongoCollection);
        System.out.println(jsonObject.toString());
        metaDataService.close();
    }
}
