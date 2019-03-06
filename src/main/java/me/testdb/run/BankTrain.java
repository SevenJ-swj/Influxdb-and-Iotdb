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

public class BankTrain {
    public static void main(String args[])
    {
        Long t1=System.currentTimeMillis();
        String path="F:\\DATASET\\数据\\bank_train";
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
                File readfile = new File(path + "\\" + filelist[i]);
                System.out.println(path + "\\" + filelist[i]);
                String[] fileinfo = readfile.getName().split("\\.");
                System.out.println("Getfile:"+readfile.getName());
                fileFormat = fileinfo[fileinfo.length - 1];
                if(readfile.getName()=="train101.csv") continue;
                BufferedReader br;
                try {
                    List<Pair<String,String>> struc=new ArrayList<Pair<String, String>>();
                    br = new BufferedReader(new FileReader(readfile));
                    String line = "";
                    Integer startLine=2;
                    while (startLine>1)
                    {
                        br.readLine();
                        startLine--;
                    }
                    while ((line = br.readLine()) != null && line != "") {
                        systime++;
                        Map<String, Double> fd = new HashMap<String, Double>();
                        Map<String, Long> fl = new HashMap<String, Long>();
                        String[] data = line.split(",");
                        String time = data[0];
                        String value = data[1];
                        String label=data[2];
                        fl.put("label",Long.valueOf(label));
                        fd.put("value",Double.valueOf(value));
                        Point p = influxDBService.generatePoint("BankTrain", time.toString(), fl, fd, null, null, null);
                        influxDBService.insertPoint(p, influxDB);
                    }
                    Long te=System.currentTimeMillis();
                    System.out.println(readfile.getName()+" used "+(te-ts)+"ms\n"+"row:"+systime);
                    result.append(readfile.getName()+" used "+(te-ts)+"ms\n"+"row:"+systime+"\n");
                } catch (Exception e) {
                    System.out.println(e);
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
