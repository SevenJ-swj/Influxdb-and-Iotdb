import me.testdb.service.InfluxDBUtil;
import org.influxdb.InfluxDB;

public class PoolTest {
    public static void main(String[] args) {
        InfluxDBUtil util = new InfluxDBUtil();
        try {
            InfluxDB conn = util.getConnection();
            if (conn != null) {
                System.out.println("我得到了一个连接");
            }
            util.CloseConnection(conn);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
