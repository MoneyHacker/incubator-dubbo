package org.apache.dubbo.config.support;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.*;
import org.apache.dubbo.config.*;
import org.apache.dubbo.config.spi.ServiceImporter;
import org.apache.dubbo.config.utils.DubboConfigUtil;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.support.AvailableCluster;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.protocol.injvm.InjvmProtocol;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import static org.apache.dubbo.common.utils.NetUtils.isInvalidLocalHost;

/**
 * Created by lvxiang@ganji.com 2018/12/10 18:16
 *
 * @author <a href="mailto:lvxiang@ganji.com">simple</a>
 */
public class DubboServiceImporter implements ServiceImporter {
    protected static final Logger logger = LoggerFactory.getLogger(DubboServiceImporter.class);
    private static final Protocol refprotocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
    private static final Cluster cluster = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();
    private static final ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();


    @Override
    public <T> T doImport(String beanName, Object bean) {
        return init((ReferenceConfig) bean);
    }

    private void checkConfig(ReferenceConfig referenceBean) {
        if (referenceBean.getApplication() == null) {
            throw new IllegalArgumentException("applicationConfig needed ");
        }
        if (referenceBean.getConsumer() == null) {
            throw new IllegalArgumentException("ConsumerConfig needed ");
        }
    }





    private <T> T init(ReferenceConfig referenceBean) {
        if (referenceBean.isInitialized()) {
            return null;
        }
        referenceBean.setInitialized(true);
        if (StringUtils.isEmpty(referenceBean.getInterfaceName())) {
            throw new IllegalStateException("<dubbo:reference interface=\"\" /> interface not allow null!");
        }
        // get consumer's global configuration
        checkConfig(referenceBean);
        DubboConfigUtil.appendProperties(referenceBean);
        if (referenceBean.getGeneric() == null && referenceBean.getConsumer() != null) {
            referenceBean.setGeneric(referenceBean.getConsumer().getGeneric());
        }
        Class<?> interfaceClass;
        String interfaceName = referenceBean.getInterfaceName();
        if (ProtocolUtils.isGeneric(referenceBean.getGeneric())) {
            interfaceClass = GenericService.class;
        } else {
            try {
                interfaceClass = Class.forName(interfaceName, true, Thread.currentThread()
                        .getContextClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            DubboConfigUtil.checkInterfaceAndMethods(interfaceClass, referenceBean.getMethods());
        }
        String resolve = System.getProperty(interfaceName);
        String resolveFile = null;
        if (resolve == null || resolve.length() == 0) {
            resolveFile = System.getProperty("dubbo.resolve.file");
            if (resolveFile == null || resolveFile.length() == 0) {
                File userResolveFile = new File(new File(System.getProperty("user.home")), "dubbo-resolve.properties");
                if (userResolveFile.exists()) {
                    resolveFile = userResolveFile.getAbsolutePath();
                }
            }
            if (resolveFile != null && resolveFile.length() > 0) {
                Properties properties = new Properties();
                FileInputStream fis = null;
                try {
                    fis = new FileInputStream(new File(resolveFile));
                    properties.load(fis);
                } catch (IOException e) {
                    throw new IllegalStateException("Unload " + resolveFile + ", cause: " + e.getMessage(), e);
                } finally {
                    try {
                        if (null != fis) {
                            fis.close();
                        }
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
                resolve = properties.getProperty(interfaceName);
            }
        }
        if (resolve != null && resolve.length() > 0) {
            // url = resolve;
            referenceBean.setUrl(resolve);
            if (logger.isWarnEnabled()) {
                if (resolveFile != null) {
                    logger.warn("Using default dubbo resolve file " + resolveFile + " replace " + interfaceName + "" + resolve + " to p2p invoke remote service.");
                } else {
                    logger.warn("Using -D" + interfaceName + "=" + resolve + " to p2p invoke remote service.");
                }
            }
        }
        DubboConfigUtil.checkApplication(referenceBean.getApplication(),referenceBean);
        DubboConfigUtil.checkStub(interfaceClass, referenceBean);
        DubboConfigUtil.checkMock(interfaceClass, referenceBean);
        Map<String, String> map = new HashMap<>();
        DubboConfigUtil.resolveAsyncInterface(interfaceClass, map, referenceBean);
        map.put(Constants.SIDE_KEY, Constants.CONSUMER_SIDE);
        map.put(Constants.DUBBO_VERSION_KEY, Version.getProtocolVersion());
        map.put(Constants.TIMESTAMP_KEY, String.valueOf(System.currentTimeMillis()));
        if (ConfigUtils.getPid() > 0) {
            map.put(Constants.PID_KEY, String.valueOf(ConfigUtils.getPid()));
        }
        if (!referenceBean.isGeneric()) {
            String revision = Version.getVersion(interfaceClass, referenceBean.getVersion());
            if (revision != null && revision.length() > 0) {
                map.put("revision", revision);
            }

            String[] methods = Wrapper.getWrapper(interfaceClass).getMethodNames();
            if (methods.length == 0) {
                logger.warn("NO method found in service interface " + interfaceClass.getName());
                map.put("methods", Constants.ANY_VALUE);
            } else {
                map.put("methods", StringUtils.join(new HashSet<String>(Arrays.asList(methods)), ","));
            }
        }
        map.put(Constants.INTERFACE_KEY, interfaceName);
        DubboConfigUtil.appendParameters(map, referenceBean.getApplication());
        DubboConfigUtil.appendParameters(map, referenceBean.getModule());
        DubboConfigUtil.appendParameters(map, referenceBean.getConsumer(), Constants.DEFAULT_KEY);
        DubboConfigUtil.appendParameters(map, this);
        Map<String, Object> attributes = null;
        if (referenceBean.getMethods() != null && !referenceBean.getMethods().isEmpty()) {
            attributes = new HashMap<>();
            List<MethodConfig> methodConfigList = referenceBean.getMethods();
            for (MethodConfig methodConfig : methodConfigList) {
                DubboConfigUtil.appendParameters(map, methodConfig, methodConfig.getName());
                String retryKey = methodConfig.getName() + ".retry";
                if (map.containsKey(retryKey)) {
                    String retryValue = map.remove(retryKey);
                    if ("false".equals(retryValue)) {
                        map.put(methodConfig.getName() + ".retries", "0");
                    }
                }
                attributes.put(methodConfig.getName(), DubboConfigUtil.convertMethodConfig2AyncInfo(methodConfig));
            }
        }

        String hostToRegistry = ConfigUtils.getSystemProperty(Constants.DUBBO_IP_TO_REGISTRY);
        if (hostToRegistry == null || hostToRegistry.length() == 0) {
            hostToRegistry = NetUtils.getLocalHost();
        } else if (isInvalidLocalHost(hostToRegistry)) {
            throw new IllegalArgumentException("Specified invalid registry ip from property:" + Constants.DUBBO_IP_TO_REGISTRY + ", value:" + hostToRegistry);
        }
        map.put(Constants.REGISTER_IP_KEY, hostToRegistry);

        T ref = createProxy(map, referenceBean);
        ConsumerModel consumerModel = new ConsumerModel(DubboConfigUtil.getUniqueServiceName(referenceBean.getInterfaceName(),referenceBean.getVersion(),referenceBean.getGroup()), ref, interfaceClass.getMethods(), attributes);
        ApplicationModel.initConsumerModel(DubboConfigUtil.getUniqueServiceName(referenceBean.getInterfaceName(),referenceBean.getVersion(),referenceBean.getGroup()), consumerModel);
        referenceBean.setRef(ref);
        return ref;
    }



    @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
    private <T> T createProxy(Map<String, String> map, ReferenceConfig referenceBean) {
        URL tmpUrl = new URL("temp", "localhost", 0, map);
        boolean isJvmRefer;
        List<URL> urls = new ArrayList<>();
        Invoker<?> invoker = null;
        if (referenceBean.isInjvm() == null) {
            if (StringUtils.isNotEmpty(referenceBean.getUrl())) { // if a url is specified, don't do local reference
                isJvmRefer = false;
            } else {
                // by default, reference local service if there is
                isJvmRefer = InjvmProtocol.getInjvmProtocol().isInjvmRefer(tmpUrl);
            }
        } else {
            isJvmRefer = referenceBean.isInjvm();
        }

        if (isJvmRefer) {
            URL url = new URL(Constants.LOCAL_PROTOCOL, NetUtils.LOCALHOST, 0, referenceBean.getInterfaceClass().getName()).addParameters(map);
            invoker = refprotocol.refer(referenceBean.getInterfaceClass(), url);
            if (logger.isInfoEnabled()) {
                logger.info("Using injvm service " + referenceBean.getInterfaceClass().getName());
            }
        } else {
            if (StringUtils.isNotEmpty(referenceBean.getUrl())) { // user specified URL, could be peer-to-peer address, or register center's address.
                String[] us = Constants.SEMICOLON_SPLIT_PATTERN.split(referenceBean.getUrl());
                if (us != null && us.length > 0) {
                    for (String u : us) {
                        URL url = URL.valueOf(u);
                        if (url.getPath() == null || url.getPath().length() == 0) {
                            url = url.setPath(referenceBean.getInterfaceName());
                        }
                        if (Constants.REGISTRY_PROTOCOL.equals(url.getProtocol())) {
                            urls.add(url.addParameterAndEncoded(Constants.REFER_KEY, StringUtils.toQueryString(map)));
                        } else {
                            urls.add(ClusterUtils.mergeUrl(url, map));
                        }
                    }
                }
            } else { // assemble URL from register center's configuration
                List<URL> us = DubboConfigUtil.loadRegistries(false,referenceBean);
                if (us != null && !us.isEmpty()) {
                    for (URL u : us) {
                        URL monitorUrl = DubboConfigUtil.loadMonitor(u,referenceBean.getMonitor(),referenceBean.getApplication());
                        if (monitorUrl != null) {
                            map.put(Constants.MONITOR_KEY, URL.encode(monitorUrl.toFullString()));
                        }
                        urls.add(u.addParameterAndEncoded(Constants.REFER_KEY, StringUtils.toQueryString(map)));
                    }
                }
                if (urls.isEmpty()) {
                    throw new IllegalStateException("No such any registry to reference " + referenceBean.getInterfaceName() + " on the consumer " + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion() + ", please config <dubbo:registry address=\"...\" /> to your spring config.");
                }
            }

            if (urls.size() == 1) {
                invoker = refprotocol.refer(referenceBean.getInterfaceClass(), urls.get(0));
            } else {
                List<Invoker<?>> invokers = new ArrayList<Invoker<?>>();
                URL registryURL = null;
                for (URL url : urls) {
                    invokers.add(refprotocol.refer(referenceBean.getInterfaceClass(), url));
                    if (Constants.REGISTRY_PROTOCOL.equals(url.getProtocol())) {
                        registryURL = url; // use last registry url
                    }
                }
                if (registryURL != null) { // registry url is available
                    // use AvailableCluster only when register's cluster is available
                    URL u = registryURL.addParameter(Constants.CLUSTER_KEY, AvailableCluster.NAME);
                    invoker = cluster.join(new StaticDirectory(u, invokers));
                } else { // not a registry url
                    invoker = cluster.join(new StaticDirectory(invokers));
                }
            }
        }

        Boolean c = referenceBean.isCheck();
        String group = referenceBean.getGroup();
        if (c == null && referenceBean.getConsumer() != null) {
            c = referenceBean.getConsumer().isCheck();
        }
        if (c == null) {
            c = true; // default true
        }
        if (c && !invoker.isAvailable()) {
            // make it possible for consumer to retry later if provider is temporarily unavailable
            referenceBean.setInitialized(false);
            throw new IllegalStateException("Failed to check the status of the service " + referenceBean.getInterfaceName()
                    + ". No provider available for the service " + (group == null ? "" : group + "/")
                    + referenceBean.getInterfaceName() + (referenceBean.getVersion() == null ? "" : ":" + referenceBean.getVersion())
                    + " from the url " + invoker.getUrl() + " to the consumer " + NetUtils.getLocalHost()
                    + " use dubbo version " + Version.getVersion());
        }
        if (logger.isInfoEnabled()) {
            logger.info("Refer dubbo service " + referenceBean.getInterfaceClass().getName() + " from url " + invoker.getUrl());
        }
        // create service proxy
        return (T) proxyFactory.getProxy(invoker);
    }


}
