package me.testdb.mongo;

import com.mongodb.client.MongoCollection;
import me.testdb.service.InfluxDBService;
import me.testdb.service.MetaDataService;
import net.sf.json.JSON;
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

public class UCR {
    public static void main(String args[])
    {
        MetaDataService metaDataService=new MetaDataService();
        MongoCollection mongoCollection=metaDataService.openCol();
        Long t1=System.currentTimeMillis();
        String path="F:\\DATASET\\UCRArchive_2018";
        //path=args[0];
        File file = null;
        String fileFormat = null;
        FileInputStream in1;
        DataInputStream data_in;


        StringBuilder result=new StringBuilder();
        String tn="";
        try {
            file = new File(path);  // file path
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
                Long ts=System.currentTimeMillis();
                File readDir = new File(path + "/" + filelist[i]);
                String[] readDirlist=readDir.list();
                JSONObject jsonObject=new JSONObject();
                jsonObject.put("token","");
                jsonObject.put("auth",1);
                jsonObject.put("dataSet","UCRsuit");
                jsonObject.put("subDataSet","UCR_"+filelist[i]);
                tn=filelist[i];
                jsonObject.put("description" ,"UCR分类数据集");
                jsonObject.put("type" ,2);
                jsonObject.put("source"  ,"");
                jsonObject.put("target"  ,"");

                JSONObject jsonObjectInType=new JSONObject();
                jsonObjectInType.put("train","TRAIN");
                jsonObjectInType.put("test","TEST");
                JSONObject jsonObjectTag=new JSONObject();
                jsonObjectTag.put("type",jsonObjectInType);

                Set<String> setclass =new HashSet<String>();
                Long setseries =0L;
                Long dataLen=0L;
                for(int j=0;j<readDirlist.length;j++) {
                    File readfile = new File(path + "\\" + filelist[i]+"\\"+readDirlist[j]);
                    //System.out.println(path + "\\" + filelist[i]);
                    String[] fileinfo = readfile.getName().split("\\.");
                    //System.out.println("Getfile:"+readfile.getName());
                    fileFormat = fileinfo[fileinfo.length - 1];
                    if (!fileFormat.equals("tsv")) continue;



                    //System.out.println("file format:" + fileFormat);
                    String tableName = fileinfo[0].split("_")[0];
                    String whatIs = fileinfo[0].split("_")[1];
                    Map<String,String> Tag=new HashMap<String, String>();
                    BufferedReader br;
                    try {
                        br = new BufferedReader(new FileReader(readfile));
                        Long rowNum=0L;
                        String line = "";
                        while ((line = br.readLine()) != null && line != "") {
                            Long systime = 0L;
                            rowNum++;
                            Map<String, Double> fd = new HashMap<String, Double>();
                            String[] data = line.split("\t");
                           // System.out.println(data.length);
                            Tag.put("class",data[0]);
                            setclass.add(data[0]);
                            dataLen=Long.valueOf(data.length);
                            Tag.put("series","series"+rowNum.toString());
                            Tag.put("type",whatIs);
                            for (int k = 1; k < data.length; k++) {

                                systime++;
                                Double t;
                                if(data[k].equals("NaN")) t=0.0;
                                else t = Double.valueOf(data[k]);
                                fd.put("value", t);
                            }
                        }
                        if(rowNum>setseries) setseries=rowNum;
                    } catch (Exception e) {
                        System.out.println("pre"+e);
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
                jsonObject.put("dbName","UCR");
                jsonObject.put("table","UCR_"+tn);



                //System.out.println(jsonObject.toString());
                metaDataService.saveMetaData(jsonObject.toString(),mongoCollection);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
        metaDataService.close();
    }
}
