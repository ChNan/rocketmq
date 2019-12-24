package org.apache.rocketmq.common.constant;

/**
 * @author CNan
 */
public class Constant {

    public interface Broker{
        /**
         * 128字节
         */
        int BYTES_128 = 128 * 1024;

        /**
         * broker服务名
         */
        String MQBROKER = "mqbroker";

        /**
         * Broke启动r监听的端口
         */
        int LISTEN_PORT = 10911;
    }

    public interface LogModule{
        String BROKER_STARTUP = "[BROKER_STARTUP]";
    }
}
