import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.CreateMode.PERSISTENT_SEQUENTIAL;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * Created by lleywyn on 17-4-29.
 */
public class Client {
    private ZooKeeper zk;
    private String Client_Id = "Client-" + new Random().nextInt(10);

    public void start() throws IOException {
        this.zk = ZKUtil.startZK(Client_Id);
        createQueueCommand("task-------0001");
    }

    private void createQueueCommand(String command) {
        createTask(command);
    }

    private void createTask(final String command) {
        zk.create(BootStrap.TASKS_PATH + "/task-", command.getBytes(), OPEN_ACL_UNSAFE, PERSISTENT_SEQUENTIAL, new AsyncCallback.StringCallback() {
            public void processResult(int rc, String path, Object ctx, String name) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        createTask(command);
                        break;
                    case OK:
                        monitorTaskStatus(name);
                }

            }
        }, null);
    }

    /**
     * 监视任务的执行状态
     *
     * @param name
     */
    private void monitorTaskStatus(final String name) {
        zk.getData(BootStrap.STATUS_PATH + "/" + name, new Watcher() {
            public void process(WatchedEvent event) {
                if (event != null && event.getType() == Event.EventType.NodeDataChanged) {
                    monitorTaskStatus(name);
                }
            }
        }, new AsyncCallback.DataCallback() {
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        monitorTaskStatus(name);
                        break;
                    case OK:
                        if (StringUtils.equals("true", new String(data)) || StringUtils.equals("false", new String(data))) {
                            System.out.println("*********************** 任务: " + name + "执行完毕, 执行结果为: " + new String(data));
                            deleteTaskStatus(name);
                        } else {
                            monitorTaskStatus(name);
                        }
                        return;
                }
            }
        }, null);
    }

    /**
     * 任务执行完毕,删除任务状态节点
     *
     * @param name
     */
    private void deleteTaskStatus(final String name) {
        zk.delete(BootStrap.STATUS_PATH + "/" + name, -1, new AsyncCallback.VoidCallback() {
            public void processResult(int rc, String path, Object ctx) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        deleteTaskStatus(name);
                }
            }
        }, null);
    }

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public void stop() throws InterruptedException {
        if (zk == null) {
            return;
        }
        ZKUtil.stop(Client_Id, zk);
    }
}
