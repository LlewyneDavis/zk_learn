import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * Created by lleywyn on 17-4-27.
 */
public class BootStrap {
    public static final String SERVER_ID = "BootServer0000000001";
    public static final String WORKERS_PATH = "/workers";
    public static final String ASSIGN_PATH = "/assign";
    public static final String TASKS_PATH = "/tasks";
    public static final String STATUS_PATH = "/status";
    private ZooKeeper zk;
    private AsyncCallback.StringCallback createDataCallBack = new AsyncCallback.StringCallback() {
        public void processResult(int i, String s, Object o, String s1) {
            String path = (String) o;
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    checkData(path);
                    break;
                case OK:
                    System.out.println(path + "创建成功！！！！！");
                    break;
                case NODEEXISTS:
                    System.out.println(path + "已经存在！！！！！");
                    break;
                default:
                    createNode(path);

            }
        }
    };
    private AsyncCallback.DataCallback checkDataCallBack = new AsyncCallback.DataCallback() {
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            String nodePath = (String) o;
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    checkData((nodePath));
                    break;
                case NONODE:
                    createNode(nodePath);
                    break;
                case NODEEXISTS:
                    System.out.println(nodePath + "创建成功！！！！！");
            }
        }
    };


    public void setUp() throws IOException, InterruptedException {
        zk = ZKUtil.startZK(SERVER_ID);
        createWorkersNode();
        createAssignNode();
        createTaskNode();
        createStatusNode();
        Thread.sleep(5000);
        ZKUtil.stop(SERVER_ID,zk);
    }

    private void createStatusNode() {
        createNode(STATUS_PATH);
    }

    private void createTaskNode() {
        createNode(TASKS_PATH);
    }

    private void createAssignNode() {
        createNode(ASSIGN_PATH);
    }

    private void createWorkersNode() {
        createNode(WORKERS_PATH);
    }


    private void createNode(String path) {
        zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createDataCallBack, path);
    }

    private void checkData(String path) {

        zk.getData(path, false, checkDataCallBack, path);
    }

}
