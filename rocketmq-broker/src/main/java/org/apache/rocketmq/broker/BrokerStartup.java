/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.rocketmq.common.constant.Constant.Broker.*;
import static org.apache.rocketmq.common.constant.Constant.LogModule.BROKER_STARTUP;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;

public class BrokerStartup {


    public static Properties properties;
    public static CommandLine commandLine;
    public static String configFile;
    public static Logger log;

    public static void main(String[] args) {

        start(createBrokerController(args));
    }

    public static void start(BrokerController controller) {
        try {

            controller.start();

            logTip(controller);
        } catch (Throwable e) {
            log.error(BROKER_STARTUP + "Broker startup failed ", e);
            System.exit(-1);
        }
    }

    private static void logTip(BrokerController controller) {
        String tip = String.format("The broker[%s,%s] boot success. serializeType=%s",
                controller.getBrokerConfig().getBrokerName(),
                controller.getBrokerAddr(),
                RemotingCommand.getSerializeTypeConfigInThisServer()
        );

        if (null != controller.getBrokerConfig().getNamesrvAddr()) {
            tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
        }

        log.info(tip);
    }

    public static BrokerController createBrokerController(String[] args) {
        try {

            setDefaultValue();

            BrokerController controller = newBrokerController(args);

            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            addShutdownHook(controller);

            return controller;
        } catch (Throwable e) {

            e.printStackTrace();
            log.error(BROKER_STARTUP + "Create broker controller failed", e);

            System.exit(-1);
        }

        return null;
    }

    private static void setDefaultValue() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            NettySystemConfig.socketSndbufSize = BYTES_128;
        }

        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            NettySystemConfig.socketRcvbufSize = BYTES_128;
        }
    }

    private static void addShutdownHook(BrokerController controller) {
        Runtime.getRuntime().addShutdownHook(new ShowdownHookRunnable("ShowdownHook", controller));
    }

    private static BrokerController newBrokerController(String[] args) throws IOException, JoranException {
        Options options = ServerUtil.buildCommandlineOptions(new Options());

        commandLine = ServerUtil.parseCmdLine(
                MQBROKER,
                args,
                buildCommandlineOptions(options),
                new PosixParser()
        );

        if (null == commandLine) {
            System.exit(-1);
        }

        BrokerConfig brokerConfig = new BrokerConfig();

        NettyServerConfig nettyServerConfig = newNettyServerConfig();

        NettyClientConfig nettyClientConfig = newNettyClientConfig();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

        if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
            int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
            messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
        }

        loadPropertiesByConfigFile(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);

        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

        brokerConfig.setRocketmqHome("E:\\B_CodeRepo_Learning\\rocketmq-2\\rocketmq-distribution");
        brokerConfig.setNamesrvAddr("localhost:9876");
        if (null == brokerConfig.getRocketmqHome()) {
            log.info( ("Please set the " + MixAll.ROCKETMQ_HOME_ENV
                    + " variable in your environment to match the location of the RocketMQ installation"));
            System.exit(-2);
        }

        validateNameServerAddr(brokerConfig);

        switch (messageStoreConfig.getBrokerRole()) {
            case ASYNC_MASTER:
            case SYNC_MASTER:
                brokerConfig.setBrokerId(MixAll.MASTER_ID);
                break;
            case SLAVE:
                if (brokerConfig.getBrokerId() > 0) break;

                log.info(BROKER_STARTUP + "Slave's brokerId must be > 0");
                System.exit(-3);
                break;
            default:
                break;
        }

        messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);

        configLogger(brokerConfig);

        printlnAllConfigItems(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);

        printlnImportantConfig(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);

        log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
        MixAll.printObjectProperties(log, brokerConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
        MixAll.printObjectProperties(log, nettyClientConfig);
        MixAll.printObjectProperties(log, messageStoreConfig);

        BrokerController controller = new BrokerController(
                brokerConfig, nettyServerConfig,
                nettyClientConfig, messageStoreConfig
        );

        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    private static NettyServerConfig newNettyServerConfig() {
        NettyServerConfig nettyServerConfig = new NettyServerConfig();

        nettyServerConfig.setListenPort(LISTEN_PORT);
        return nettyServerConfig;
    }

    private static NettyClientConfig newNettyClientConfig() {
        NettyClientConfig nettyClientConfig = new NettyClientConfig();

        nettyClientConfig.setUseTLS(
                Boolean.parseBoolean(
                        System.getProperty(
                                TLS_ENABLE,
                                String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING)
                        )
                )
        );
        return nettyClientConfig;
    }

    private static void configLogger(BrokerConfig brokerConfig) throws JoranException {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();

        //用于定义Logback的配置机制
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);

        lc.reset();

        configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");
    }

    private static void printlnAllConfigItems(BrokerConfig brokerConfig,
                                              NettyServerConfig nettyServerConfig,
                                              NettyClientConfig nettyClientConfig,
                                              MessageStoreConfig messageStoreConfig) {
        if (!commandLine.hasOption('p')) return;

        Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
        MixAll.printObjectProperties(console, brokerConfig);
        MixAll.printObjectProperties(console, nettyServerConfig);
        MixAll.printObjectProperties(console, nettyClientConfig);
        MixAll.printObjectProperties(console, messageStoreConfig);
        System.exit(0);
    }

    private static void printlnImportantConfig(BrokerConfig brokerConfig,
                                               NettyServerConfig nettyServerConfig,
                                               NettyClientConfig nettyClientConfig,
                                               MessageStoreConfig messageStoreConfig) {
        if (!commandLine.hasOption('m')) return ;

        Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
        MixAll.printObjectProperties(console, brokerConfig, true);
        MixAll.printObjectProperties(console, nettyServerConfig, true);
        MixAll.printObjectProperties(console, nettyClientConfig, true);
        MixAll.printObjectProperties(console, messageStoreConfig, true);
        System.exit(0);
    }

    private static void validateNameServerAddr(BrokerConfig brokerConfig) {
        String nameServerAddr = brokerConfig.getNamesrvAddr();
        if (null == nameServerAddr) return;

        try {
            String[] addrArray = nameServerAddr.split(";");
            for (String addr : addrArray) {
                RemotingUtil.string2SocketAddress(addr);
            }
        } catch (Exception e) {
            log.info( "The Name Server Address[%s] illegal, please set it as follows, " +
                            "\"127.0.0.1:9876;192.168.0.1:9876\"%n", nameServerAddr);
            System.exit(-3);
        }
    }

    private static void loadPropertiesByConfigFile(BrokerConfig brokerConfig,
                                                   NettyServerConfig nettyServerConfig,
                                                   NettyClientConfig nettyClientConfig,
                                                   MessageStoreConfig messageStoreConfig) throws IOException {
        if (!commandLine.hasOption('c')) return;

        String file = commandLine.getOptionValue('c');

        if (file == null) return ;

        configFile = file;
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        properties = new Properties();
        properties.load(in);

        properties2SystemEnv(properties);
        MixAll.properties2Object(properties, brokerConfig);
        MixAll.properties2Object(properties, nettyServerConfig);
        MixAll.properties2Object(properties, nettyClientConfig);
        MixAll.properties2Object(properties, messageStoreConfig);

        BrokerPathConfigHelper.setBrokerConfigPath(file);

        in.close();
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }

        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    static class ShowdownHookRunnable extends Thread {
        private volatile boolean hasShutdown = false;
        private AtomicInteger shutdownTimes = new AtomicInteger(0);

        private BrokerController controller;

        ShowdownHookRunnable(String name, BrokerController controller){
            super(name);
            this.controller = controller;
        }

        @Override
        public void run() {
            synchronized (this) {
                log.info(BROKER_STARTUP + "Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());

                if (this.hasShutdown) return ;

                this.hasShutdown = true;
                long beginTime = System.currentTimeMillis();

                controller.shutdown();

                long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                log.info(BROKER_STARTUP + "Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
            }
        }
    }
}
