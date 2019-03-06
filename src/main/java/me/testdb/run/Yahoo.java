package me.testdb.run;

import me.testdb.service.InfluxDBService;

public class Yahoo {
    public static void main(String[] args)throws Exception
    {

        //if(args.length <4) {System.out.println("arg Error");return;}
        //String filePath=args[0];
        //String measurement=args[1];
        //String BatchNum=args[2];
        //String BatchTime=args[3];
        String filePath="F:\\DATASET\\数据\\yahoo\\A3Benchmark\\A3Benchmark_all.csv";
        //System.out.println("Start:"+filePath+" "+measurement+" "+BatchNum+" "+BatchTime);
        InfluxDBService influxDBService=new InfluxDBService();
        //influxDBService.setBatchNum(Integer.valueOf(BatchNum));
        //influxDBService.setBatchTime(Integer.valueOf(BatchTime));
        Long t1=System.currentTimeMillis();
        influxDBService.batchInsert(filePath,"Yahoo_A34all","bgdata");
//        influxDBService.batchInsert("F:\\tmpdata\\data\\ecg0606_1.csv","ecgTest");
        Long t2=System.currentTimeMillis();
        System.out.println("ExcuteTime(S):");
        System.out.println((t2-t1)/1000.0);
        return;
    }
}
