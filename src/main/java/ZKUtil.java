import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Created by lleywyn on 17-4-27.
 */
public class ZKUtil {
    static ZooKeeper startZK(final String serverId) throws IOException {
        return new ZooKeeper(StartUp.CONNECT_STRING, StartUp.SESSION_TIMEOUT, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println(serverId + "建立链接！！！");
                System.out.println(watchedEvent);
            }
        });
    }

    public static void stop(String serverId, ZooKeeper zk) throws InterruptedException {
        if (zk == null) {
            System.out.println(serverId + "has stopped!!!!!!");
        }
        System.out.println(serverId + "中断和ZKServer的链接！！");
        zk.close();
    }
}
