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

public class GoogleCluster {
    public static Map<String,List<Pair<String,String >>> getHead2()
    {
        Map<String,List<Pair<String,String >>> result=new HashMap<String, List<Pair<String, String>>>();
        List<Pair<String,String>> struc=new ArrayList<Pair<String, String>>();

        struc.add(new Pair<String,String>().make_pair("time","Long"));
        struc.add(new Pair<String,String>().make_pair("missing_info","Long"));
        struc.add(new Pair<String,String>().make_pair("job_ID","Long"));
        struc.add(new Pair<String,String>().make_pair("event_type","Long"));
        struc.add(new Pair<String,String>().make_pair("user","String"));
        struc.add(new Pair<String,String>().make_pair("scheduling_class","Long"));
        struc.add(new Pair<String,String>().make_pair("job_name","String"));
        struc.add(new Pair<String,String>().make_pair("logical_job_name","String"));
        result.put("job_events",struc);
        struc=new ArrayList<Pair<String, String>>();

        struc.add(new Pair<String,String>().make_pair("time","Long"));
        struc.add(new Pair<String,String>().make_pair("missing_info","Long"));
        struc.add(new Pair<String,String>().make_pair("job_ID","Long"));
        struc.add(new Pair<String,String>().make_pair("task_index","Long"));
        struc.add(new Pair<String,String>().make_pair("machine_ID","Long"));
        struc.add(new Pair<String,String>().make_pair("event_type","Long"));
        struc.add(new Pair<String,String>().make_pair("user","String"));
        struc.add(new Pair<String,String>().make_pair("scheduling_class","Long"));
        struc.add(new Pair<String,String>().make_pair("priority","Long"));
        struc.add(new Pair<String,String>().make_pair("CPU_request","Double"));
        struc.add(new Pair<String,String>().make_pair("memory_request","Double"));
        struc.add(new Pair<String,String>().make_pair("disk_space_request","Double"));
        struc.add(new Pair<String,String>().make_pair("different_machines_restriction","Boolean"));
        result.put("task_events",struc);
        struc=new ArrayList<Pair<String, String>>();

        struc.add(new Pair<String,String>().make_pair("time","Long"));
        struc.add(new Pair<String,String>().make_pair("machine_ID","Long"));
        struc.add(new Pair<String,String>().make_pair("event_type","Long"));
        struc.add(new Pair<String,String>().make_pair("platform_ID","String"));
        struc.add(new Pair<String,String>().make_pair("CPUs","Double"));
        struc.add(new Pair<String,String>().make_pair("Memory","Double"));
        result.put("machine_events",struc);
        struc=new ArrayList<Pair<String, String>>();

        struc.add(new Pair<String,String>().make_pair("time","Long"));
        struc.add(new Pair<String,String>().make_pair("machine_ID","Long"));
        struc.add(new Pair<String,String>().make_pair("attribute_name","String"));
        struc.add(new Pair<String,String>().make_pair("attribute_value","String"));
        struc.add(new Pair<String,String>().make_pair("attribute_deleted","Boolean"));
        result.put("machine_attributes",struc);
        struc=new ArrayList<Pair<String, String>>();

        struc.add(new Pair<String,String>().make_pair("time","Long"));
        struc.add(new Pair<String,String>().make_pair("job_ID","Long"));
        struc.add(new Pair<String,String>().make_pair("task_index","Long"));
        struc.add(new Pair<String,String>().make_pair("comparison_operator","Long"));
        struc.add(new Pair<String,String>().make_pair("attribute_name","String"));
        struc.add(new Pair<String,String>().make_pair("attribute_value","String"));
        result.put("task_constraints",struc);
        struc=new ArrayList<Pair<String, String>>();

        struc.add(new Pair<String,String>().make_pair("start_time","Long"));
        struc.add(new Pair<String,String>().make_pair("end_time","Long"));
        struc.add(new Pair<String,String>().make_pair("job_ID","Long"));
        struc.add(new Pair<String,String>().make_pair("task_index","Long"));
        struc.add(new Pair<String,String>().make_pair("machine_ID","Long"));
        struc.add(new Pair<String,String>().make_pair("CPU_rate","Double"));
        struc.add(new Pair<String,String>().make_pair("canonical_memory_usage","Double"));
        struc.add(new Pair<String,String>().make_pair("assigned_memory_usage","Double"));
        struc.add(new Pair<String,String>().make_pair("unmapped_page_cache","Double"));
        struc.add(new Pair<String,String>().make_pair("total_page_cache","Double"));
        struc.add(new Pair<String,String>().make_pair("maximum_memory_usage","Double"));
        struc.add(new Pair<String,String>().make_pair("disk_I/O_time","Double"));
        struc.add(new Pair<String,String>().make_pair("local_disk_space_usage","Double"));
        struc.add(new Pair<String,String>().make_pair("maximum_CPU_rate","Double"));
        struc.add(new Pair<String,String>().make_pair("maximum_disk_IO_time","Double"));
        struc.add(new Pair<String,String>().make_pair("cycles_per_instruction","Double"));
        struc.add(new Pair<String,String>().make_pair("memory_accesses_per_instruction","Double"));
        struc.add(new Pair<String,String>().make_pair("sample_portion","Double"));
        struc.add(new Pair<String,String>().make_pair("aggregation_type","Boolean"));
        struc.add(new Pair<String,String>().make_pair("sampled_CPU_usage","Double"));
        result.put("task_usage",struc);

        return result;
    }

    public static void main(String args[]) throws Exception
    {
        MetaDataService metaDataService=new MetaDataService();
        MongoCollection mongoCollection=metaDataService.openCol();
        Long t1=System.currentTimeMillis();
        String path="F:\\DATASET\\数据集\\GoogleCluster\\clusterdata-2011-2";
        File file = null;
        String fileFormat = null;
        FileInputStream in1;
        DataInputStream data_in;

        Map<String,List<Pair<String,String>>> head=getHead2();
        try {
            file = new File(path);  // file path
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("token","");
                jsonObject.put("auth",1);
                jsonObject.put("dataSet", "GoogleCluster");
                jsonObject.put("subDataSet",filelist[i]);

                jsonObject.put("description", "GoogleCluster");
                jsonObject.put("type", 2);
                jsonObject.put("source", new JSONArray());
                jsonObject.put("target", "");
                jsonObject.put("tag",new JSONObject());
                JSONObject jsonObjectInType = new JSONObject();

                JSONObject jsonObjectTag = new JSONObject();
                JSONArray jsonArrayF = new JSONArray();
                JSONArray jsonArrayT = new JSONArray();
                File readDir = new File(path + "/" + filelist[i]);
                if(readDir.isFile()) continue;
                Long ts=System.currentTimeMillis();
                Long systime = 0L;
                String[] readDirlist=readDir.list();
                Map<String,String> tag=new HashMap<String, String>();
                tag.put("tag",filelist[i]);

                List<Pair<String,String> > struc=head.get(filelist[i]);
                for(int j=0;j<struc.size();j++)
                {
                    jsonArrayF.add(struc.get(j).getFirst());
                    jsonArrayT.add(struc.get(j).getSecond());
                }
                jsonObject.put("fieldName",jsonArrayF);
                jsonObject.put("fieldType",jsonArrayT);
                jsonObject.put("columns",struc.size()-1);
                jsonObject.put("rows", new JSONArray());
                jsonObject.put("dbType",1);
                jsonObject.put("dbName","GoogleCluster");
                jsonObject.put("table",filelist[i]);
                System.out.println(jsonObject.toString());
                metaDataService.saveMetaData(jsonObject.toString(),mongoCollection);

            }
        } catch (Exception e) {

        }

    }
}
