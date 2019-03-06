import me.testdb.service.InfluxDBService;
import me.testdb.service.InfluxLoadDataService;
import org.influxdb.InfluxDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class test {
    public static void main(String[] args)
    {

        Map<String,List<String>> tagMap=new HashMap<String, List<String>>();
        List<String> t=new ArrayList<String>();
        t.add("1");
        t.add("2");
        t.add("3");
        //tagMap.put("class",t);
        List<String> tt=new ArrayList<String>();
        tt.add("TRAIN");
        tt.add("TEST");
        //tagMap.put("type",tt);
        List<String> ttt=new ArrayList<String>();
        ttt.add("series1");
        ttt.add("series2");
        ttt.add("series3");
        //tagMap.put("series",ttt);
        List<String> fieldList=new ArrayList<String>();
        fieldList.add("auto_field1");
        Long readNumber;
        Boolean withHeader;
        Boolean withLabel;
        Boolean withTimestamp;String timestampFormatStr;
       // InfluxLoadDataService writeService=new InfluxLoadDataService();
       // writeService.loadData("./test.csv","test","stl_demo",tagMap,fieldList,10L,false,false,true,"yyyy/MM/dd HH:mm");

        //writeService.doWrite("bgdata","select * from \"PAMAP1\" where \"tag\"='Indoor' and \"subject\"='subject3' limit 10");
    }
}
