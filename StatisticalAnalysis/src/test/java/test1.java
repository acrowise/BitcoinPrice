import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * @author ngt
 * @create 2020-12-20 23:56
 */
public class test1 {
    public static void main(String[] args) {

        Set<HostAndPort> nodes = new HashSet<>();
        nodes.add(new HostAndPort("127.0.0.1", 7001));

        nodes.add(new HostAndPort("127.0.0.1", 7002));

        nodes.add(new HostAndPort("127.0.0.1", 7003));

        nodes.add(new HostAndPort("127.0.0.1", 7004));

        nodes.add(new HostAndPort("127.0.0.1", 7005));

        nodes.add(new HostAndPort("127.0.0.1", 7006));

        JedisCluster cluster = new JedisCluster(nodes);

    }
}
