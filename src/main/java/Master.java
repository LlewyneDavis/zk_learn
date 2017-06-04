import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.apache.zookeeper.AsyncCallback.DataCallback;
import static org.apache.zookeeper.AsyncCallback.StringCallback;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * Created by lleywyn on 17-4-27.
 */
public class Master {

    public static final String MASTER_PATH = "/master";
    private String SERVER_ID = "server_" + new Random().nextInt(100);
    private boolean isLeader;
    private ZooKeeper zk;
    private MasterStatusEnum status;
    private NodeCache<String> workersCache = new NodeCache<String>();
    private NodeCache<String> tasksCache = new NodeCache<String>();

    public void runForMasterAsyn() throws IOException {
        System.out.println(SERVER_ID + "开始创建主节点!!!!");
        zk = new ZooKeeper(StartUp.CONNECT_STRING, StartUp.SESSION_TIMEOUT, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    Master.this.createMasterNodeAsyn();
                }
            }
        });

    }

    private void createMasterNodeAsyn() {
        zk.create(MASTER_PATH, SERVER_ID.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new StringCallback() {
            public void processResult(int i, String s, Object o, String s1) {
                switch (KeeperException.Code.get(i)) {
                    case CONNECTIONLOSS:
                        Master.this.checkMasterAsyn();
                        return;
                    case OK:
                        Master.this.isLeader = true;
                        Master.this.status = MasterStatusEnum.ELECTED;
                        takeLeader();
                        masterExists();
                        System.out.println("------------------------------  " + Master.this.SERVER_ID + "被选举为主节点!!!");
                        break;
                    case NODEEXISTS:
                        masterExists();
                        Master.this.checkMasterAsyn();
                        break;
                    case NONODE:
                        Master.this.createMasterNodeAsyn();
                        break;
                    default:
                        System.out.println("创建主节点出错~~~~~");
                }
            }
        }, null);
    }

    private void checkMasterAsyn() {
        this.zk.getData(MASTER_PATH, false, new DataCallback() {
            public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
                switch (KeeperException.Code.get(i)) {
                    case CONNECTIONLOSS:
                        checkMasterAsyn();
                        break;
                    case NONODE:
                        createMasterNodeAsyn();
                        break;
                    case OK:
                        String serverId = new String(bytes);
                        if (StringUtils.equals(Master.this.SERVER_ID, serverId)) {
                            Master.this.isLeader = true;
                            Master.this.status = MasterStatusEnum.ELECTED;
                            takeLeader();
                            System.out.println("------------------------------  " + Master.this.SERVER_ID + "被选举为主节点!!!");
                        } else {
                            Master.this.isLeader = false;
                            Master.this.status = MasterStatusEnum.NOT_ELECTED;
                            System.out.println("******************************  " + Master.this.SERVER_ID + "没有被选举为主节点!!! ~_~ " + "主节点为 : " + serverId);
                        }
                        masterExists(); //无论该节点是否已经被选举为主节点,都要监视主节点状态(防止主节点被误删除)
                    default:
                        break;
                }
            }
        }, null);
    }

    /**
     * 被选举为主节点之后，调用该方法行驶主节点权
     */
    private void takeLeader() {
        //获取worker节点,并且创建assign下的worker节点
        fetchWorkersAndPrepareAssign();
        //获取任务节点,并且分配任务
        fetchTasksAndAssign();
    }


    private void createAssignWorker(String worker) {
        final String path = BootStrap.ASSIGN_PATH + "/" + worker;
        zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new StringCallback() {
            public void processResult(int i, String s, Object o, String s1) {
                switch (KeeperException.Code.get(i)) {
                    case CONNECTIONLOSS:
                        createAssignWorker((String) o);
                        break;
                    default:
                        return;
                }
            }
        }, worker);
    }

    /**
     * 获取任务节点数据
     */
    private void fetchTasksAndAssign() {
        zk.getChildren(BootStrap.TASKS_PATH, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    fetchTasksAndAssign();
                }
            }
        }, new AsyncCallback.ChildrenCallback() {
            public void processResult(int i, String s, Object o, List<String> list) {
                switch (KeeperException.Code.get(i)) {
                    case CONNECTIONLOSS:
                        fetchTasksAndAssign();
                    case OK:
                        Master.this.tasksCache.refresh(list);
                        assignTasks();
                        return;
                }
            }
        }, null);
    }

    /**
     * 分配任务
     */
    private void assignTasks() {
        // TODO: 17-5-1 现在假设每次分配的任务一定会有worker去执行,并且会给出最终执行结果,但是实际上会有以下情况:分配给一个worker的任务还没有执行,该worker节点挂掉了,这个版本先不考虑异常分支,也不去考虑各个worker节点的工作状态
        final List<String> tasks = tasksCache.getAddedNodes();
        if (CollectionUtils.isNotEmpty(tasks)) {
            return;
        }
        while (true) {
            final List<String> wokers = workersCache.getNodes();
            if (CollectionUtils.isNotEmpty(wokers)) {
                continue;
            }
            for (int i = 0; i < tasks.size(); i++) {
                final int wokerNO = i % wokers.size();
                final String worker = wokers.get(wokerNO);
                createTaskStatus(tasks.get(i));
                assignToWorker(tasks.get(i), worker);
            }
            break;
        }
    }

    /**
     * 创建任务的状态节点
     *
     * @param task
     */
    private void createTaskStatus(final String task) {
        zk.create(BootStrap.STATUS_PATH + "/" + task, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new StringCallback() {
            public void processResult(int rc, String path, Object ctx, String name) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        createTaskStatus(task);
                }
            }
        }, null);
    }

    private void assignToWorker(final String task, final String worker) {
        zk.create(BootStrap.ASSIGN_PATH + "/" + worker + "/" + task, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new StringCallback() {
            public void processResult(int rc, String path, Object ctx, String name) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        assignToWorker(task, worker);
                        return;
                }
            }
        }, null);
    }

    /**
     * 获取worker节点
     */
    private void fetchWorkersAndPrepareAssign() {
        zk.getChildren(BootStrap.WORKERS_PATH, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    fetchWorkersAndPrepareAssign();
                }
            }
        }, new AsyncCallback.ChildrenCallback() {
            public void processResult(int i, String s, Object o, List<String> list) {
                switch (KeeperException.Code.get(i)) {
                    case CONNECTIONLOSS:
                        fetchWorkersAndPrepareAssign();
                        break;
                    case OK:
                        //缓存wokers节点到本地
                        Master.this.workersCache.refresh(list);
                        //处理新增节点和删除节点
                        prepareAssign();
                        return;

                }
            }
        }, null);
    }

    /**
     * 每次监听到wokers节点变更,需要处理新增节点和删除节点
     */
    private void prepareAssign() {
        //对于新增节点,需要将节点添加到 assign路径下面,以便接受任务
        if (CollectionUtils.isNotEmpty(workersCache.getAddedNodes())) {
            for (String worker : workersCache.getAddedNodes()) {
                createAssignWorker(worker);
            }
        }
        //对于删除的节点,需要做两步,(1)将该节点下的为执行完成的任务重新分配给其他的worker节点 (2)删除该worker在assign下的节点
        if (CollectionUtils.isNotEmpty(workersCache.getDeletedNodes())) {
            for (String deletedWorker : workersCache.getDeletedNodes()) {
                reAssign(deletedWorker);
                deleteInvalidWorker(deletedWorker);
            }
        }
    }

    /**
     * 重新分配该节点下未完成的任务给别的节点
     *
     * @param deletedWorker
     */
    private void reAssign(String deletedWorker) {
        // TODO: 17-4-30
    }

    /**
     * 删除失效的节点在assign下的路径
     *
     * @param deletedWorker
     */
    private void deleteInvalidWorker(String deletedWorker) {
        final String path = BootStrap.ASSIGN_PATH + "/" + deletedWorker;
        zk.delete(path, -1, new AsyncCallback.VoidCallback() {
            public void processResult(int rc, String path, Object ctx) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        deleteInvalidWorker((String) ctx);
                        return;
                }
            }
        }, deletedWorker);
    }

    /**
     * 在创建主节点的时候发现主节点已经存在,则调用该方法监视该节点
     */
    private void masterExists() {
        zk.exists(MASTER_PATH, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                //如果已经存在的主节点被删除了  则再次运行createMasterNodeAsyn(),创建主节点
                if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                    Master.this.createMasterNodeAsyn();
                }
            }
        }, new AsyncCallback.StatCallback() {
            public void processResult(int i, String s, Object o, Stat stat) {
                switch (KeeperException.Code.get(i)) {
                    case CONNECTIONLOSS:
                        masterExists();
                        break;
                    case OK: //exist 返回成功,但是主节点被删除
                        if (stat == null) {
                            Master.this.createMasterNodeAsyn();
                        }
                        break;
                    default:
                        checkMasterAsyn();


                }
            }
        }, null);
    }


    public void stop() throws InterruptedException {
        ZKUtil.stop(SERVER_ID, zk);
    }

    public MasterStatusEnum getStatus() {
        return status;
    }


    public static enum MasterStatusEnum {
        ELECTED("elected", "被选举为主节点"),

        NOT_ELECTED("not_elected", "没有被选举为主节点");

        private String code;
        private String desc;

        private MasterStatusEnum(String code, String desc) {
            this.code = code;
            this.desc = desc;
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(String desc) {
            this.desc = desc;
        }
    }


}
