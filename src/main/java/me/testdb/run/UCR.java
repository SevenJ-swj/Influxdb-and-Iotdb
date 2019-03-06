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

public class UCR {
    public static void main(String args[])
    {
        Long t1=System.currentTimeMillis();
        String path="F:\\DATASET\\UCRArchive_2018";
        //path=args[0];
        File file = null;
        String fileFormat = null;
        FileInputStream in1;
        DataInputStream data_in;
        InfluxDBService influxDBService = new InfluxDBService();
        InfluxDB influxDB=influxDBService.getConnection();
        influxDB.setDatabase("UCR");
        influxDB.enableBatch(5000, 50000, TimeUnit.MILLISECONDS);
        StringBuilder result=new StringBuilder();
        try {

            file = new File(path);  // file path
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
                Long ts=System.currentTimeMillis();
                File readDir = new File(path + "/" + filelist[i]);
                String[] readDirlist=readDir.list();
                for(int j=0;j<readDirlist.length;j++) {
                    File readfile = new File(path + "\\" + filelist[i]+"\\"+readDirlist[j]);
                    System.out.println(path + "\\" + filelist[i]);
                    String[] fileinfo = readfile.getName().split("\\.");
                    System.out.println("Getfile:"+readfile.getName());
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
                            Tag.put("series","series"+rowNum.toString());
                            Tag.put("type",whatIs);
                            for (int k = 1; k < data.length; k++) {
                                systime++;
                                Double t;
                                if(data[k].equals("NaN")) t=0.0;
                                else t = Double.valueOf(data[k]);
                                fd.put("value", t);
                                Point p = influxDBService.generatePoint("UCR_"+tableName, systime.toString(), null, fd, null, null, Tag);

                                influxDBService.insertPoint(p, influxDB);
                            }
                        }
                        Long te=System.currentTimeMillis();
                        System.out.println(readfile.getName()+" used "+(te-ts)+"ms\n"+"row:"+rowNum);
                        result.append(readfile.getName()+" used "+(te-ts)+"ms\n"+"row:"+rowNum+"\n");
                    } catch (Exception e) {

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
