package me.testdb.mongo;

import com.mongodb.client.MongoCollection;
import me.testdb.service.InfluxDBService;
import me.testdb.service.MetaDataService;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class Hime {
    public static void main(String[] args) throws  Exception
    {

        //if(args.length <4) {System.out.println("arg Error");return;}
        //String filePath=args[0];
        //String measurement=args[1];
        //String BatchNum=args[2];
        //String BatchTime=args[3];

        MetaDataService metaDataService=new MetaDataService();
        MongoCollection mongoCollection=metaDataService.openCol();

        InfluxDBService influxDBService=new InfluxDBService();

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("token","");
        jsonObject.put("auth",1);
        jsonObject.put("dataSet", "bgdata");
        jsonObject.put("subDataSet", "Hime_Freezer");
        jsonObject.put("description", "Hime数据集");
        jsonObject.put("type", 2);
        jsonObject.put("source", new JSONArray());
        jsonObject.put("target", "");


        JSONArray jsonArrayField=new JSONArray();
        JSONArray jsonArrayType= new JSONArray();
        jsonArrayField.add("value");
        jsonArrayType.add("Double");
        jsonObject.put("tag",new JSONObject());

        jsonObject.put("fieldName",jsonArrayField);
        jsonObject.put("fieldType",jsonArrayType);
        jsonObject.put("columns",jsonArrayField.size());
        jsonObject.put("rows", new JSONArray());
        jsonObject.put("dbType",1);
        jsonObject.put("dbName","bgdata");
        jsonObject.put("table","Hime_Freezer");
        System.out.println(jsonObject.toString());
        metaDataService.saveMetaData(jsonObject.toString(),mongoCollection);

        return;
    }
}
