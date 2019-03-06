package me.testdb.run;

import me.testdb.service.InfluxDBService;

public class BatchInsert {
    public static void main(String args[]) throws Exception
    {
        InfluxDBService influxDBService=new InfluxDBService();
        String path="F:\\DATASET\\DataSourcr\\单指标\\交易类数据\\中国银联单指标(count&responce_cost.csv";
        String measurement="中国银联单指标";
        influxDBService.batchInsert(path,measurement,"eoitek_sharepoint");

    }
}
