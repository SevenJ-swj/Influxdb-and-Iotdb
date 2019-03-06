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

public class Satellite {
    public static void main(String args[])
    {
        Long t1=System.currentTimeMillis();
        String path="F:\\DATASET\\Satellite\\桌面联试试验数据";
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
                File readfile = new File(path + "/" + filelist[i]);
                System.out.println(path + "/" + filelist[i]);
                String[] fileinfo = readfile.getName().split("\\.");
                System.out.println("Getfile:"+readfile.getName());
                fileFormat = fileinfo[fileinfo.length - 1];
                if (fileFormat.equals("xlsx")) continue;
                //System.out.println("file format:" + fileFormat);
                String tagName = fileinfo[0].split("_")[0];
                System.out.println("taglName:" + tagName);
                String whatIs = fileinfo[0].split("_")[1];
                if (whatIs.equals("table")) continue;
                Map<String, String> Tag = new HashMap<String, String>();
                Tag.put("tag", tagName);
                File readTable = new File(path + "/" + tagName + "_table.txt");
                List<String> colName = new ArrayList<String>();
                List<Pair<String, String>> struc = new ArrayList<Pair<String, String>>();
                BufferedReader br;
                try {
                    br = new BufferedReader(new FileReader(readTable));
                    String line = "";
                    while ((line = br.readLine()) != null && line != "") {
                        Pair<String, String> p = new Pair<String, String>();
                        p.setFirst(line);
                        p.setSecond("Double");
                        struc.add(p);
                    }
                    struc.add(new Pair<String, String>().make_pair(tagName, "Tag"));
                    br.close();

                    Structure structure = new Structure();
                    structure.setHasHeader(-1);
                    structure.setStartLine(1);
                    structure.setHasTime(false);
                    structure.setFileStruct(struc);
                    br = new BufferedReader(new FileReader(readfile));
                    line = "";
                    while ((line = br.readLine()) != null && line != "") {
                        systime++;
                        Map<String, Double> fd = new HashMap<String, Double>();
                        String[] data = line.split(",");
                        for (int k = 0; k < data.length; k++) {
                            Double t = Double.valueOf(data[k]);
                            fd.put(struc.get(k).getFirst(), t);
                        }
                        Point p = influxDBService.generatePoint("Satellite", systime.toString(), null, fd, null, null, Tag);
                        influxDBService.insertPoint(p, influxDB);
                    }
                    Long te=System.currentTimeMillis();
                    System.out.println(readfile.getName()+" used "+(te-ts)+"ms\n"+"row:"+systime);
                    result.append(readfile.getName()+" used "+(te-ts)+"ms\n"+"row:"+systime+"\n");
                } catch (Exception e) {

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
