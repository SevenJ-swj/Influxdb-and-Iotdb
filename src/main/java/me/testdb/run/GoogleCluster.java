package me.testdb.run;

import com.sun.org.apache.xpath.internal.operations.Bool;
import me.testdb.service.InfluxDBService;
import me.testdb.tool.Pair;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.io.*;
import java.util.*;
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

    public static void main(String args[])
    {
        Long t1=System.currentTimeMillis();
        String path="F:\\DATASET\\数据集\\GoogleCluster\\clusterdata-2011-2";
        path=args[0];
        File file = null;
        String fileFormat = null;
        FileInputStream in1;
        DataInputStream data_in;
        InfluxDBService influxDBService = new InfluxDBService();
        InfluxDB influxDB=influxDBService.getConnection();
        influxDB.setDatabase("GoogleCluster");
        influxDB.enableBatch(5000, 50000, TimeUnit.MILLISECONDS);
        StringBuilder result=new StringBuilder();
        Map<String,List<Pair<String,String>>> head=getHead2();
        try {
            file = new File(path);  // file path
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
                System.out.println(filelist[i]);
                File readDir = new File(path + "/" + filelist[i]);
                if(readDir.isFile()) continue;
                Long ts=System.currentTimeMillis();
                Long systime = 0L;
                String[] readDirlist=readDir.list();
                Map<String,String> tag=new HashMap<String, String>();
                tag.put("tag",filelist[i]);

                List<Pair<String,String> > struc=head.get(filelist[i]);

                for(int j=0;j<readDirlist.length;j++){


                    File readfile=new File(path + "/" + filelist[i]+"/"+readDirlist[j]);
                    String[] fileinfo = readfile.getName().split("\\.");
                    System.out.println(path + "/" + filelist[i]+"/"+readDirlist[j]);
                    if(!fileinfo[fileinfo.length-1].equals("csv")) continue;
                    BufferedReader br;
                    Long localTime=0L;
                    try {
                        br = new BufferedReader(new FileReader(readfile));
                        String line="";
                        while ((line = br.readLine()) != null && line != "") {
                            systime++;
                            localTime++;
                            Map<String, Double> fd = new HashMap<String, Double>();
                            Map<String, Long> fl = new HashMap<String, Long>();
                            Map<String, String> fs = new HashMap<String, String>();
                            Map<String, Boolean> fb= new HashMap<String, Boolean>();


                            String[] data = line.split(",");
                            String time=data[0];
                            int id=1;
                            if(filelist[0].equals("task_usage")) {time=localTime.toString();id=0;}

                            for(;id<data.length;id++)
                            {
                                if(data[id].equals("NaN")||data[id].equals("")) continue;
                                //System.out.println(data[id]);
                                //System.out.println(data.length+" "+struc.size());
                                //System.out.println(struc.get(id).getFirst()+" "+struc.get(id).getSecond());
                                if(struc.get(id).getSecond().equals("String")) fs.put(struc.get(id).getFirst(),data[id]);
                                if(struc.get(id).getSecond().equals("Double")) fd.put(struc.get(id).getFirst(),Double.valueOf(data[id]));
                                if(struc.get(id).getSecond().equals("Boolean")) fb.put(struc.get(id).getFirst(),Boolean.valueOf(data[id]));
                                if(struc.get(id).getSecond().equals("Long")) fl.put(struc.get(id).getFirst(),Long.valueOf(data[id]));
                                //System.out.println(data[id]);
                            }

                            Point p = influxDBService.generatePoint(filelist[i], time, fl, fd, fs, fb, null);
                            //System.out.println(p.toString());
                            influxDBService.insertPoint(p, influxDB);
                        }
                        } catch (Exception e) {
                        System.out.println(e);
                    }
                }
                Long te=System.currentTimeMillis();
                System.out.println(readDir.getName()+" "+filelist[i]+" used "+(te-ts)+"ms\n"+"row:"+systime);
                result.append(readDir.getName()+" "+filelist[i]+" used "+(te-ts)+"ms\n"+"row:"+systime+"\n");

            }
        } catch (Exception e) {
            System.out.println("open file failed,please check path\n" + e);
            Long t2=System.currentTimeMillis();
            System.out.println("Excute:"+(t2-t1));
        }
        Long t2=System.currentTimeMillis();
        System.out.println("Total Excute:"+(t2-t1));
        System.out.println(result.toString());
        influxDB.close();
    }
}
