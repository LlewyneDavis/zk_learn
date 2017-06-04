import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Created by lleywyn on 17-4-29.
 */
public class Worker {
    private String WORKER_ID = "worker-" + new Random().nextInt(100);
    private String PATH = BootStrap.WORKERS_PATH + "/" + WORKER_ID;
    private ZooKeeper zk;
    private String status = WorkerStatus.IDLE;
    private NodeCache<String> tasksCache = new NodeCache<String>();


    public void start() throws IOException {
        this.zk = new ZooKeeper(StartUp.CONNECT_STRING, StartUp.SESSION_TIMEOUT, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    register();
                }
            }
        });
    }

    private void register() {
        zk.create(PATH, WorkerStatus.IDLE.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new AsyncCallback.StringCallback() {
            public void processResult(int i, String s, Object o, String s1) {
                switch (KeeperException.Code.get(i)) {
                    case CONNECTIONLOSS:
                        register();
                        break;
                    case NODEEXISTS:
                    case OK:
                        System.out.println("工作节点创建成功！！！ work_id = " + WORKER_ID);
                        //监视assign下的该节点是否有任务
                        monitorTasks();
                        return;
                    default:
                        System.out.println("创建工作节点出错！！！");

                }
            }
        }, null);
    }

    /**
     * 监视assign下的该工作节点是否有任务分配
     */
    private void monitorTasks() {
        zk.getChildren(BootStrap.ASSIGN_PATH + "/" + this.WORKER_ID + "/", new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    monitorTasks();
                }
            }
        }, new AsyncCallback.ChildrenCallback() {
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        monitorTasks();
                        break;
                    case OK:
                        if (CollectionUtils.isNotEmpty(children)) {
                            tasksCache.refresh(children);
                            executeTasks();
                        } else {
                            monitorTasks();
                        }
                        return;
                }
                // TODO: 17-5-1
            }
        }, null);
    }

    /**
     * 执行分配的任务
     */
    private void executeTasks() {
        // TODO: 17-5-1 此处需要考虑执行任务的线程模型,该版本暂且不考虑
        if (CollectionUtils.isEmpty(tasksCache.getAddedNodes())) {
            return;
        }
        for (String task : tasksCache.getAddedNodes()) {
            //执行task
            executeTask(task);
        }
    }

    /**
     * 执行任务
     *
     * @param task
     */
    private void executeTask(final String task) {
        zk.getData(BootStrap.TASKS_PATH + "/" + task, false, new AsyncCallback.DataCallback() {
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        executeTask((String) ctx);
                        break;
                    case OK:
                        System.out.println("-------------------开始执行任务: " + task + "---------------------");
                        System.out.println(new String(data));
                        System.out.println("-------------------任务: " + task + "执行完毕 ---------------------");
                        updateTaskstatus(task, true);
                        break;
                    default:
                        updateTaskstatus(task, false);

                }
            }
        }, task);
    }

    /**
     * 执行完任务之后更新任务执行状态
     *
     * @param task
     * @param b
     */
    private void updateTaskstatus(String task, boolean b) {
        updateExecuteStatus(task, b);
        deleteExecutesTask(task);
    }

    private void deleteExecutesTask(final String task) {
        zk.delete(BootStrap.TASKS_PATH + "/" + task, -1, new AsyncCallback.VoidCallback() {
            public void processResult(int rc, String path, Object ctx) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        deleteExecutesTask(task);
                }
            }
        }, null);
    }

    private void updateExecuteStatus(final String task, final boolean b) {
        zk.setData(BootStrap.STATUS_PATH + "/" + task, String.valueOf(b).getBytes(), -1, new AsyncCallback.StatCallback() {
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        updateExecuteStatus(task, b);
                }
            }
        }, null);
    }

    public void stop() throws InterruptedException {
        ZKUtil.stop(WORKER_ID, zk);
    }

    public static class WorkerStatus {

        public static final String IDLE = "Idle";
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    private void updateStatus(String status) {
        if (!StringUtils.equals(status, this.status)) {
            return;
        }
        //don't check the data version
        zk.setData(PATH, status.getBytes(), -1, new AsyncCallback.StatCallback() {
            public void processResult(int i, String s, Object o, Stat stat) {
                switch (KeeperException.Code.get(i)) {
                    case CONNECTIONLOSS:
                        updateStatus((String) o);
                        return;
                }
            }
        }, status);
    }
}
