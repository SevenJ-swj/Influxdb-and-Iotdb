package me.testdb.run;

import me.testdb.service.InfluxDBService;
import me.testdb.tool.Pair;
import me.testdb.tool.Structure;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.awt.*;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args)
    {

        //if(args.length <4) {System.out.println("arg Error");return;}
        //String filePath=args[0];
        //String measurement=args[1];
        //String BatchNum=args[2];
        //String BatchTime=args[3];
        String filePath="C:\\Users\\swjch\\Desktop\\新建文件夹 (2)\\segment\\segment_small_demo.csv";
        //System.out.println("Start:"+filePath+" "+measurement+" "+BatchNum+" "+BatchTime);
        InfluxDBService influxDBService=new InfluxDBService();
        //influxDBService.setBatchNum(Integer.valueOf(BatchNum));
        //influxDBService.setBatchTime(Integer.valueOf(BatchTime));
        Long t1=System.currentTimeMillis();
        try{
            influxDBService.batchInsert("F:\\tmpdata\\data\\ecg0606_1.csv","ecgTest");
            influxDBService.batchInsert(filePath,"segment_small_demo");
        }catch (Exception e){

        }
        Long t2=System.currentTimeMillis();
        System.out.println("ExcuteTime(S):");
        System.out.println((t2-t1)/1000.0);
        return;
    }
   public static void main2(String args[])
   {
       Long t1=System.currentTimeMillis();
       String path="F:\\DATASET\\Stress_fuben";
       File file = null;
       String fileFormat = null;
       FileInputStream in1;
       DataInputStream data_in;
       InfluxDBService influxDBService = new InfluxDBService();
       InfluxDB influxDB=influxDBService.getConnection();
       influxDB.setDatabase("test");
       influxDB.enableBatch(50000, 50000, TimeUnit.MILLISECONDS);
       try {
           file = new File(path);  // file path
           String[] filelist=file.list();
           for(int i=0;i<filelist.length;i++) {
                Long systime=1L;
               File readfile=new File(path+"\\"+filelist[i]);
               String[] fileinfo = readfile.getName().split("\\.");
               System.out.println(readfile.getName());
               fileFormat = fileinfo[fileinfo.length - 1];
               System.out.println("file format:" + fileFormat);
               String colName=fileinfo[0].split("_")[1];
               System.out.println("colName:"+colName);
               in1 = new FileInputStream(readfile);
               data_in = new DataInputStream(in1);
               System.out.println(data_in.available());
               int total = data_in.available();
               Integer cnt = 0;
               Integer len = 50000*4;
               Float t;
               byte[] tmp = new byte[len];
               for (int k = 0; k < total; k += len) {
                   int size = data_in.read(tmp);
                   for (int index = 0; index < size; index += 4) {
                       int l;
                       l = tmp[index + 0];
                       l &= 0xff;
                       l |= ((long) tmp[index + 1] << 8);
                       l &= 0xffff;
                       l |= ((long) tmp[index + 2] << 16);
                       l &= 0xffffff;
                       l |= ((long) tmp[index + 3] << 24);

                       t = Float.intBitsToFloat(l);
                       Map<String, Double> fd = new HashMap<String, Double>();
                       fd.put(colName, Double.valueOf(t));
                       Point p = influxDBService.generatePoint("Stressp", systime.toString(), null, fd, null, null, null);
                       influxDBService.insertPoint (p,influxDB);
                       systime++;
                       if(systime%1000000==0) System.out.println(systime);
                       cnt++;
                       //if(cnt%100000==0)System.out.println(cnt);
                   }
               }
           }
          /* int l;
           int index=0;
           l = tmp[index + 0];
           l &= 0xff;
           l |= ((long) tmp[index + 1] << 8);
           l &= 0xffff;
           l |= ((long) tmp[index + 2] << 16);
           l &= 0xffffff;
           l |= ((long) tmp[index + 3] << 24);
           t = Float.intBitsToFloat(l);
           System.out.println(t);*/
          /* while((t=data_in.readFloat())!=null) {
               //System.out.println(t);
               cnt++;
               if(cnt%10000==0) System.out.println(cnt);
           }
           System.out.println(cnt);*/
       } catch (Exception e) {
           System.out.println("open file failed,please check path\n" + e);
           Long t2=System.currentTimeMillis();
           System.out.println("Excute:"+(t2-t1));
       }
       Long t2=System.currentTimeMillis();
       System.out.println("Excute:"+(t2-t1));
   }
   /*
    public static void main(String[] args)
    {
        String path="F:\\DATASET\\humidity";
        InfluxDBService influxDBService = new InfluxDBService();
        influxDBService.setBatchNum(50000);
        InfluxDB influxDB=influxDBService.getConnection();
        influxDB.setDatabase("bgdata");
        Long systime=1L;
        File file = null;
        String fileFormat = null;
        FileInputStream in1;
        DataInputStream data_in;
        try {
            file = new File(path);  // file path
            String[] fileinfo = file.getName().split("\\.");
            System.out.println(file.getName());
            fileFormat = fileinfo[fileinfo.length - 1];
            System.out.println("file format:" + fileFormat);
            in1 =new FileInputStream(file);
            data_in = new DataInputStream(in1);
            Float t;
            influxDB.enableBatch(50000, 50000, TimeUnit.MILLISECONDS);
            while((t=data_in.readFloat())!=null) {
                //System.out.println(t);
                Map<String, Double> fd = new HashMap<>();
                fd.put("humidity", Double.valueOf(t));
                Point p = influxDBService.generatePoint("Humidity", systime.toString(), null, fd, null, null, null);
                influxDBService.insertPoint (p,influxDB);
                systime++;
                if(systime%1000000==0) System.out.println(systime);
            }
            System.out.println("cnt:"+systime);
            influxDB.close();
        } catch (Exception e) {
            System.out.println("open file failed,please check path\n" + e);
            // return null;
        }
    }*/

    public static void main_insert_stress(String[] args)
    {
        //"select * from hktest where auto_field1>=0 and auto_field2 <100000"
        InfluxDBService influxDBService=new InfluxDBService();
        //influxDBService.parseFile("E:\\stl.csv");
        List<Pair<String,String>> fileStruct=new ArrayList<Pair<String, String>>();
        Pair<String, String> tmp = new Pair<String, String>();
        tmp.setFirst("lagan");
        tmp.setSecond("Double");
        fileStruct.add(tmp);
        Structure structure=new Structure();
        structure.setHasHeader(-1);
        structure.setStartLine(1);
        structure.setHasTime(false);
        structure.setFileStruct(fileStruct);
        Long t1=System.currentTimeMillis();
        influxDBService.setBatchNum(50000);
        try{
            influxDBService.batchInsert("F:\\DATASET\\Stress\\STRESSP_lagan.csv","Stressp","bgdata",structure);
        }catch (Exception e){
            System.out.println(e);
        }
        //List<List<String>> ls1=influxDBService.queryByTime("test","hktest",String);
//        List<List<String>> ls1=influxDBService.query("test",args[0]);
        //influxDBService.queryByTime("test",)
        //influxDBService.queryByTime()
        Long t2=System.currentTimeMillis();
        System.out.println("ExcuteTime(S):");
        System.out.println((t2-t1)/1000.0);
//        if(ls1==null||ls1.size()==0) System.out.println("row:0\ncol:0\ntotal:0");
//        else System.out.println("row:"+ls1.size()+"\ncol:"+(ls1.get(0).size())+"\ntotal:"+(ls1.size()* ls1.get(0).size()));
//        return;
    }
}
