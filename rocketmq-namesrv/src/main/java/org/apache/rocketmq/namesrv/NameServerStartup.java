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
package org.apache.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NameServerConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;

//fixme 这段代码整体上看来还是比较混乱，不够简洁。
public class NameServerStartup {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    public static final int LISTEN_PORT = 9876;

    public static Properties properties = null;
    public static CommandLine commandLine = null;

    public static void main(String[] args) {
        main0(args);
    }

    private static void main0(String[] args) {

        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        try {
            Options options = ServerUtil.buildCommandlineOptions(new Options());

            commandLine = ServerUtil.parseCmdLine("mqnamesrv", args,
                    buildCommandlineOptions(options), new PosixParser()
            );

            if (null == commandLine) {
                System.exit(-1);
                return;
            }

            // RocketMQ寻址服务配置
            NameServerConfig nameServerConfig = new NameServerConfig();

            NettyServerConfig nettyServerConfig = new NettyServerConfig();

            nettyServerConfig.setListenPort(LISTEN_PORT);

            doHasConfigFileOption(nameServerConfig, nettyServerConfig);

            if (commandLine.hasOption('p')) {
                MixAll.printObjectProperties(null, nameServerConfig);
                MixAll.printObjectProperties(null, nettyServerConfig);
                System.exit(0);
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), nameServerConfig);

            nameServerConfig.setRocketmqHome("E:\\B_CodeRepo_Learning\\rocketmq-2\\distribution");
            if (null == nameServerConfig.getRocketmqHome()) {
                log.info("Please set the %s variable in your environment to match " +
                    "the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
                System.exit(-2);
            }

            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(loggerContext);
            loggerContext.reset();
            configurator.doConfigure(nameServerConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");

            Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

            MixAll.printObjectProperties(log, nameServerConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);

            final NameServerController controller = new NameServerController(nameServerConfig, nettyServerConfig);

            // remember all configs to prevent discard
            controller.getConfiguration().registerConfig(properties);

            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            Runtime.getRuntime().addShutdownHook(
                    new ShutdownHookThread(log, (Callable<Void>) () -> {
                        controller.shutdown();
                        return null;
                    })
            );

            controller.start();

            String tip = "The Name Server boot success. serializeType="
                + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            log.info(tip + "%n");

        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void doHasConfigFileOption(NameServerConfig nameServerConfig,
                                              NettyServerConfig nettyServerConfig) throws IOException {
        if (!hasConfigFileOption()) return;

        String file = commandLine.getOptionValue('c');

        if (file == null) return;

        InputStream in = new BufferedInputStream(new FileInputStream(file));
        properties = new Properties();
        properties.load(in);

        MixAll.properties2Object(properties, nameServerConfig);
        MixAll.properties2Object(properties, nettyServerConfig);

        nameServerConfig.setConfigStorePath(file);

        log.info("load config properties file OK, " + file + "%n");
        in.close();
    }

    private static boolean hasConfigFileOption() {
        return commandLine.hasOption('c');
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
