package me.testdb.run;

import me.testdb.service.InfluxDBService;
import me.testdb.tool.Pair;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PAMAP2 {
    public static List<Pair<String,String >> getHead2()
    {
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

    public static void main(String args[])
    {
        Long t1=System.currentTimeMillis();
        String path="F:\\DATASET\\PAMAP\\PAMAP2_Dataset";
        File file = null;
        String fileFormat = null;
        FileInputStream in1;
        DataInputStream data_in;
        InfluxDBService influxDBService = new InfluxDBService();
        InfluxDB influxDB=influxDBService.getConnection();
        influxDB.setDatabase("bgdata");
        influxDB.enableBatch(5000, 50000, TimeUnit.MILLISECONDS);
        StringBuilder result=new StringBuilder();
        List<Pair<String,String>> struc=getHead2();
        try {
            file = new File(path);  // file path
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
                File readDir = new File(path + "\\" + filelist[i]);
                String[] readDirlist=readDir.list();
                Map<String,String> tag=new HashMap<String, String>();
                tag.put("tag",filelist[i]);
                for(int j=0;j<readDirlist.length;j++){
                    Long systime = 0L;
                    Long ts=System.currentTimeMillis();
                    File readfile=new File(path + "\\" + filelist[i]+"\\"+readDirlist[j]);
                    String[] fileinfo = readfile.getName().split("\\.");
                    System.out.println(path + "\\" + filelist[i]+"\\"+readDirlist[j]);
                    tag.put("subject",fileinfo[0]);
                    BufferedReader br;
                    try {
                        br = new BufferedReader(new FileReader(readfile));
                        String line="";
                        while ((line = br.readLine()) != null && line != "") {
                            systime++;
                            Map<String, Double> fd = new HashMap<String, Double>();
                            Map<String, Long> fl = new HashMap<String, Long>();
                            String[] data = line.split(" ");
                            String time=String.valueOf(((Double.valueOf(data[0]))*1000L)).split("\\.")[0];

                            Long activityId;
                            if(data[1].contains(".")) activityId=Long.valueOf(data[1].split("\\.")[0]);
                            else activityId=Long.valueOf(data[1]);

                            fl.put(struc.get(1).getFirst(),activityId);
                            for(int id=2;id<data.length;id++)
                            {
                                if(data[id].equals("NaN")) data[id]="0";
                                fd.put(struc.get(id).getFirst(),Double.valueOf(data[id]));
                                //if(struc.get(id).getFirst().equals("heartRate")) System.out.println(data[id]);
                            }

                            Point p = influxDBService.generatePoint("PAMAP2", time, fl, fd, null, null, tag);
                            //System.out.println(p.toString());
                            influxDBService.insertPoint(p, influxDB);
                        }
                        Long te=System.currentTimeMillis();
                        System.out.println(readDir.getName()+" "+readfile.getName()+" used "+(te-ts)+"ms\n"+"row:"+systime);
                        result.append(readDir.getName()+" "+readfile.getName()+" used "+(te-ts)+"ms\n"+"row:"+systime+"\n");
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                }
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
