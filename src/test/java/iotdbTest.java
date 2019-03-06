import me.testdb.service.IotdbService;
import net.sf.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class iotdbTest {

    public  static  void main(String[] args) throws Exception{
        IotdbService iotdbService = new IotdbService();
       // iotdbService.batchInsert("C:\\Users\\swjch\\Desktop\\新建文件夹 (2)\\classify\\classify_demo_TEST.csv",
         //       "classify","test",true);
       List<String> fieldList=new ArrayList<>();
        fieldList.add("val");
       iotdbService.loadData("E:\\load.csv","test","classify",
                JSONObject.fromObject("\"tag\":{}"),fieldList,0L,false,true,false,"");
    }
}
