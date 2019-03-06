package me.testdb.mongo;

import com.mongodb.client.MongoCollection;
import me.testdb.service.InfluxDBService;
import me.testdb.service.MetaDataService;
import me.testdb.tool.DBUtil;
import me.testdb.tool.Structure;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class Yahoo {
    public static void main(String[] args)
    {
        DBUtil dbUtil=new DBUtil();
        MetaDataService metaDataService=new MetaDataService();
        MongoCollection mongoCollection=metaDataService.openCol();
        //if(args.length <4) {System.out.println("arg Error");return;}
        //String filePath=args[0];
        //String measurement=args[1];
        //String BatchNum=args[2];
        //String BatchTime=args[3];
        String filePath="F:\\DATASET\\数据\\yahoo\\A3Benchmark\\A3Benchmark_all.csv";

        InfluxDBService influxDBService=new InfluxDBService();
        Structure structure=dbUtil.parseFile(filePath);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("token","");
        jsonObject.put("auth",1);
        jsonObject.put("dataSet", "bgdata");
        jsonObject.put("subDataSet", "Yahoo_A34all");
        jsonObject.put("description", "Yahoo数据集");
        jsonObject.put("type", 2);
        jsonObject.put("source", new JSONArray());
        jsonObject.put("target", "");


        JSONArray jsonArrayField=new JSONArray();
        JSONArray jsonArrayType= new JSONArray();
        jsonObject.put("tag",new JSONObject());
        for(int i=0;i<structure.getFileStruct().size();i++)
        {
            if(structure.getFileStruct().get(i).getFirst().equals("timestamps")) continue;
            jsonArrayField.add(structure.getFileStruct().get(i).getFirst());
            jsonArrayType.add(structure.getFileStruct().get(i).getSecond());
        }
        jsonObject.put("fieldName",jsonArrayField);
        jsonObject.put("fieldType",jsonArrayType);
        jsonObject.put("columns",Long.valueOf(jsonArrayField.size()));
        jsonObject.put("rows",new JSONArray());
        jsonObject.put("dbType",1);
        jsonObject.put("dbName","bgdata");
        jsonObject.put("table","Yahoo_A34all");
        System.out.println(jsonObject.toString());
        metaDataService.saveMetaData(jsonObject.toString(),mongoCollection);

        return;
    }
}
