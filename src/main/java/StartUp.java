import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

/**
 * Created by lleywyn on 17-4-27.
 */
public class StartUp {
    public static final String CONNECT_STRING = "127.0.0.1:2181";
    public static final int SESSION_TIMEOUT = 5000;

    public static void main(String[] args) throws IOException, InterruptedException {
        new BootStrap().setUp();
        //start masters
        Master[] masters = {new Master(), new Master()};
        startMasters(masters);

        //start workers
        Worker[] workers = {new Worker(), new Worker()};

        //start client
        Client client = new Client();
        client.start();

        startWorkers(workers);

        byte[] bytes = new byte[100];
        System.in.read(bytes);
        String command = new String(bytes);

        if (StringUtils.equals("stop", command)) {
            //stop masters
            stopMasters(masters);

            //stop workers
            stopWorkers(workers);

            //stop client
            client.stop();
        }
    }

    private static void stopWorkers(Worker[] workers) throws InterruptedException {
        if (ArrayUtils.isEmpty(workers)) {
            return;
        }
        for (Worker worker : workers) {
            worker.stop();
        }
    }

    private static void startWorkers(Worker[] workers) throws IOException {
        if (ArrayUtils.isEmpty(workers)) {
            return;
        }
        for (Worker worker : workers) {
            worker.start();
        }
    }

    private static void stopMasters(Master[] masters) throws InterruptedException {
        if (ArrayUtils.isEmpty(masters)) {
            return;
        }
        for (Master master : masters) {
            master.stop();
        }
    }

    private static void startMasters(Master[] masters) throws IOException {
        if (ArrayUtils.isEmpty(masters)) {
            return;
        }
        for (Master master : masters) {
            master.runForMasterAsyn();
        }
    }

}
