package me.testdb.mongo;

import com.mongodb.client.MongoCollection;
import me.testdb.service.InfluxDBService;
import me.testdb.service.MetaDataService;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Classify_demo {
    public static void main(String args[])
    {
        String filePath="C:\\Users\\swjch\\Desktop\\新建文件夹 (2)\\classify";
        //System.out.println("Start:"+filePath+" "+measurement+" "+BatchNum+" "+BatchTime);

        File file = null;
        String fileFormat = null;
        FileInputStream in1;
        DataInputStream data_in;

        StringBuilder result=new StringBuilder();
        try {


            file = new File(filePath);  // file path
            String[] filelist = file.list();
            JSONObject jsonObject=new JSONObject();
            jsonObject.put("token","");
            jsonObject.put("auth",1);
            jsonObject.put("dataSet","test");
            jsonObject.put("subDataSet","classify_demo");
            jsonObject.put("description" ,"classify_demo");
            jsonObject.put("type" ,2);
            jsonObject.put("source"  ,new JSONArray());
            jsonObject.put("target"  ,"");

            JSONObject jsonObjectInType=new JSONObject();
            jsonObjectInType.put("train","TRAIN");
            jsonObjectInType.put("test","TEST");
            JSONObject jsonObjectTag=new JSONObject();
            jsonObjectTag.put("type",jsonObjectInType);

            Set<String> setclass=new HashSet<String>();
            Long setseries=0L;
            for(int i=0;i<filelist.length;i++) {

                File readfile = new File(filePath + "\\" + filelist[i]);
                System.out.println(filePath + "\\" + filelist[i]);
                String[] fileinfo = readfile.getName().split("\\.");
                System.out.println("Getfile:" + readfile.getName());
                fileFormat = fileinfo[fileinfo.length - 1];
                if (!fileFormat.equals("csv")) continue;
                //System.out.println("file format:" + fileFormat);
                String tableName = fileinfo[0].split("_")[0] + "_" +fileinfo[0].split("_")[1];
                String whatIs = fileinfo[0].split("_")[2];
                System.out.println(tableName+" "+whatIs);



                Map<String, String> Tag = new HashMap<String, String>();

                BufferedReader br;
                try {
                    br = new BufferedReader(new FileReader(readfile));
                    Long rowNum = 0L;
                    String line = "";
                    while ((line = br.readLine()) != null && line != "") {
                        rowNum++;
                        String[] data = line.split(",");
                        // System.out.println(data.length);
                        setclass.add(data[0]);

                        if(rowNum>setseries) setseries=rowNum;
                    }
                } catch (Exception e) {
                    System.out.println(e);

                }

            }
            JSONObject jsonInClass=new JSONObject();
            Integer t=0;
            for(String tt:setclass )
            {
                t++;
                jsonInClass.put("label"+t.toString(),tt);
            }
            //System.out.println(jsonInClass.toString());
            jsonObjectTag.put("label",jsonInClass);
            //System.out.println(jsonObjectTag.toString());
            JSONObject jsonInSeries=new JSONObject();
            for (Long st=1L;st<=setseries;st++)
            {
                jsonInSeries.put("series"+st.toString(),"series"+st.toString());
            }

            //System.out.println(jsonObjectTag);
            jsonObjectTag.put("series",jsonInSeries);
            jsonObject.put("tag",jsonObjectTag);
            //System.out.println(jsonObject.toString());
            JSONArray jsonArray=new JSONArray();
            jsonArray.add("value");
            jsonObject.put("fieldName",jsonArray);

            JSONArray jsonArrayType=new JSONArray();
            jsonArrayType.add("Double");
            jsonObject.put("fieldType",jsonArrayType);
            jsonObject.put("columns",1L);
            jsonObject.put("rows",new JSONArray());
            jsonObject.put("dbType",1);
            jsonObject.put("dbName","test");
            jsonObject.put("table","classify_demo");
            System.out.println(jsonObject.toString());
            MetaDataService metaDataService=new MetaDataService();
            MongoCollection mongoCollection=metaDataService.openCol();
            metaDataService.saveMetaData(jsonObject.toString(),mongoCollection);
            metaDataService.close();
        } catch (Exception e) {
            System.out.println(e);
        }
        System.out.println("end");
        //return;

    }
}
