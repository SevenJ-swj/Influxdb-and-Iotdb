package me.testdb.mongo;

import com.mongodb.client.MongoCollection;
import me.testdb.service.InfluxDBService;
import me.testdb.service.MetaDataService;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class Stl_demo {
    public static void main(String[] args)
    {

        //if(args.length <4) {System.out.println("arg Error");return;}
        //String filePath=args[0];
        //String measurement=args[1];
        //String BatchNum=args[2];
        //String BatchTime=args[3];


        InfluxDBService influxDBService=new InfluxDBService();

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("token","");
        jsonObject.put("auth",1);
        jsonObject.put("dataSet", "test");
        jsonObject.put("subDataSet", "stl_demo");
        jsonObject.put("description", "stl_demo");
        jsonObject.put("type", 2);
        jsonObject.put("source", new JSONArray());
        jsonObject.put("target", "");


        JSONArray jsonArrayField=new JSONArray();
        JSONArray jsonArrayType= new JSONArray();
        jsonArrayField.add("auto_field1");
        jsonArrayType.add("Double");
        jsonObject.put("tag",new JSONObject());

        jsonObject.put("fieldName",jsonArrayField);
        jsonObject.put("fieldType",jsonArrayType);
        jsonObject.put("columns",jsonArrayField.size());
        jsonObject.put("rows", new JSONArray());
        jsonObject.put("dbType",1);
        jsonObject.put("dbName","test");
        jsonObject.put("table","stl_demo");
        System.out.println(jsonObject.toString());
        MetaDataService metaDataService=new MetaDataService();
        MongoCollection mongoCollection=metaDataService.openCol();
        metaDataService.saveMetaData(jsonObject.toString(),mongoCollection);
        metaDataService.close();
        return;
    }
}
