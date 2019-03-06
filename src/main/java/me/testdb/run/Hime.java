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

public class Hime {

    public static void main(String args[]) {
        Long t1 = System.currentTimeMillis();
        String path = "F:\\DATASET\\data-from-peng\\新建文件夹\\dataset\\HumanY.txt";
        File file = null;
        String fileFormat = null;
        FileInputStream in1;
        DataInputStream data_in;
        InfluxDBService influxDBService = new InfluxDBService();
        InfluxDB influxDB = influxDBService.getConnection();
        influxDB.setDatabase("bgdata");
        influxDB.enableBatch(5000, 50000, TimeUnit.MILLISECONDS);
        StringBuilder result = new StringBuilder();
        try {
            file = new File(path);  // file path

            Long ts = System.currentTimeMillis();
            Long systime = 0L;

            System.out.println(path + "\\" + file.getName());

            BufferedReader br;
            try {
                List<Pair<String, String>> struc = new ArrayList<Pair<String, String>>();
                br = new BufferedReader(new FileReader(file));
                String line = "";
                while ((line = br.readLine()) != null && line != "") {
                    systime++;
                    Map<String, Double> fd = new HashMap<String, Double>();
                    fd.put("value", Double.valueOf(line));
                    Point p = influxDBService.generatePoint("Hime_HumanY", systime.toString(), null, fd, null, null, null);
                    influxDBService.insertPoint(p, influxDB);
                }
                Long te = System.currentTimeMillis();
                System.out.println(file.getName() + " used " + (te - ts) + "ms\n" + "row:" + systime);
                result.append(file.getName() + " used " + (te - ts) + "ms\n" + "row:" + systime + "\n");
            } catch (Exception e) {
                System.out.println(e);
            }
        } catch (Exception e) {
            System.out.println("open file failed,please check path\n" + e);
            Long t2 = System.currentTimeMillis();
            System.out.println("Excute:" + (t2 - t1));
        }
        Long t2 = System.currentTimeMillis();
        System.out.println("Total Excute:" + (t2 - t1));
        System.out.println(result.toString());
        influxDB.close();
    }
}
