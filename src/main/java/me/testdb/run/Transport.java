package me.testdb.run;

import me.testdb.service.InfluxDBService;
import me.testdb.tool.Pair;
import me.testdb.tool.Structure;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Transport {
    public static void main(String args[])
    {
        Long t1=System.currentTimeMillis();
        String path="F:\\DATASET\\Transport";
        path=args[0];
        File file = null;
        String fileFormat = null;
        FileInputStream in1;
        DataInputStream data_in;
        InfluxDBService influxDBService = new InfluxDBService();
        InfluxDB influxDB=influxDBService.getConnection();
        influxDB.setDatabase("bgdata");
        influxDB.enableBatch(5000, 50000, TimeUnit.MILLISECONDS);
        StringBuilder result=new StringBuilder();
        try {
            file = new File(path);  // file path
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
                Long ts=System.currentTimeMillis();
                Long systime = 0L;
                File readDir = new File(path + "/" + filelist[i]);
                String[] readDirlist=readDir.list();
                for(int j=0;j<readDirlist.length;j++){
                    File readfile=new File(path + "/" + filelist[i]+"/"+readDirlist[j]);
                    String[] fileinfo = readfile.getName().split("\\.");
                    String[] tagAndCol=fileinfo[0].split("_");
                    String tagName=tagAndCol[0];
                    String col=tagAndCol[1];
                    if(tagAndCol.length>2){
                        tagName=tagAndCol[0]+"_"+tagAndCol[1];
                        col=tagAndCol[2];
                    }
                    System.out.println(tagName+"\n"+col);
                    Map<String, String> Tag = new HashMap<String, String>();
                    Tag.put("tag", tagName);
                    BufferedReader br;
                    try {
                        Structure structure = new Structure();
                        structure.setHasHeader(-1);
                        structure.setStartLine(9);
                        structure.setHasTime(false);

                        br = new BufferedReader(new FileReader(readfile));
                        String line = "";
                        int h=8;
                        while(h>0)
                        {
                            h--;
                            br.readLine();
                        }
                        while ((line = br.readLine()) != null && line != "") {
                            systime++;
                            Map<String, Double> fd = new HashMap<String, Double>();
                            Map<String, Integer> fi = new HashMap<String, Integer>();
                            String[] data = line.split(",");
                            String time=data[0];
                            fd.put(col+"_Double",Double.valueOf(data[1]));
                            fd.put(col+"_Float",Double.valueOf(data[2]));

                            Point p = influxDBService.generatePoint("Transport", time, null, fd, null, null, Tag);
                            influxDBService.insertPoint(p, influxDB);
                        }

                    } catch (Exception e) {

                    }
                }
                Long te=System.currentTimeMillis();
                System.out.println(readDir.getName()+" used "+(te-ts)+"ms\n"+"row:"+systime);
                result.append(readDir.getName()+" used "+(te-ts)+"ms\n"+"row:"+systime+"\n");

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
