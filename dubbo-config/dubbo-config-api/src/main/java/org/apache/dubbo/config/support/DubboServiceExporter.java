package org.apache.dubbo.config.support;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ClassHelper;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.ArgumentConfig;
import org.apache.dubbo.config.MethodConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.config.invoker.DelegateProviderMetaDataInvoker;
import org.apache.dubbo.config.spi.ServiceExporter;
import org.apache.dubbo.config.utils.DubboConfigUtil;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.ConfiguratorFactory;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ProviderModel;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.lang.reflect.Method;
import java.net.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.utils.NetUtils.*;

/**
 * Created by lvxiang2018/12/10 18:14
 *
 * @author <a href="mailto:278076999@qq.com">simple</a>
 */
public class DubboServiceExporter implements ServiceExporter {
    protected static final Logger logger = LoggerFactory.getLogger(DubboServiceExporter.class);
    private static final ScheduledExecutorService delayExportExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("DubboServiceDelayExporter", true));
    private static final Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();

    private static final ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    @Override
    public void doExport(String beanName, Object bean) {
        //todo 将Bean 暴露和引入的逻辑抽取为单独模块
        doExport((ServiceConfig) bean);
    }

    /**
     * 检查整体配置参数是否合法
     */
    private void checkApplicationConfig(ServiceConfig serviceConfig) {
        if (serviceConfig.getApplication() == null) {
            throw new IllegalArgumentException("ApplicationConfig needed!");
        }
        if (serviceConfig.getApplication().getProviderConfig() == null) {
            throw new IllegalArgumentException("ProviderConfig needed!");
        }
    }
    protected synchronized void doExport(ServiceConfig serviceConfig) {
        if (serviceConfig.isUnexported()) {
            throw new IllegalStateException("Already unexported!");
        }
        if (serviceConfig.isExported()) {
            return;
        }
        serviceConfig.setExport(true);
        String interfaceName = serviceConfig.getInterface();
        if (interfaceName == null || interfaceName.length() == 0) {
            throw new IllegalStateException("<dubbo:service interface=\"\" /> interface not allow null!");
        }
        //todo 需要考虑局部配置与全局配置的融合关系
//        module = serviceConfig.getApplication().getProviderConfig().getModule();
//        registries = application.getRegistries();
//        monitor =  application.getMonitor();
//        protocols =  application.getProviderConfig().getProtocols();
        Class<?> interfaceClass = null;
        if (serviceConfig.getRef() instanceof GenericService) {
            interfaceClass = GenericService.class;
            if (StringUtils.isEmpty(serviceConfig.getGeneric())) {
                serviceConfig.setGeneric(Boolean.TRUE.toString());
            }
        } else {
            try {
                interfaceClass = Class.forName(interfaceName, true, Thread.currentThread()
                        .getContextClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            DubboConfigUtil.checkInterfaceAndMethods(interfaceClass, serviceConfig.getMethods());
            checkRef(serviceConfig);
            //serviceConfig.setGeneric(Boolean.FALSE.toString());

        }
        if (serviceConfig.getLocal() != null) {
            if ("true".equals(serviceConfig.getLocal())) {
                serviceConfig.setLocal(interfaceName + "Local");
            }
            Class<?> localClass;
            try {
                localClass = ClassHelper.forNameWithThreadContextClassLoader(serviceConfig.getLocal());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            if (!interfaceClass.isAssignableFrom(localClass)) {
                throw new IllegalStateException("The local implementation class " + localClass.getName()
                        + " not implement interface " + interfaceName);
            }
        }
        if (serviceConfig.getStub() != null) {
            if ("true".equals(serviceConfig.getStub())) {
                serviceConfig.setStub(interfaceName + "Stub");
            }
            Class<?> stubClass;
            try {
                stubClass = ClassHelper.forNameWithThreadContextClassLoader(serviceConfig.getStub());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            if (!interfaceClass.isAssignableFrom(stubClass)) {
                throw new IllegalStateException("The stub implementation class " + stubClass.getName() + " not implement interface " + interfaceName);
            }
        }
        DubboConfigUtil.checkApplication(serviceConfig.getApplication(),serviceConfig);
        DubboConfigUtil.checkRegistry(serviceConfig);
        DubboConfigUtil.checkProtocol(serviceConfig);
        DubboConfigUtil.appendProperties(serviceConfig);
        DubboConfigUtil.checkStub(interfaceClass,serviceConfig);
        DubboConfigUtil.checkMock(interfaceClass,serviceConfig);
        if (StringUtils.isEmpty(serviceConfig.getPath())) {
            serviceConfig.setPath(interfaceName);
        }
        doExportUrls(serviceConfig);
        ProviderModel providerModel = new ProviderModel(
                DubboConfigUtil.getUniqueServiceName(serviceConfig.getInterfaceName(),serviceConfig.getVersion(),serviceConfig.getGroup()),
                serviceConfig.getRef(), interfaceClass);
        ApplicationModel.initProviderModel(DubboConfigUtil.getUniqueServiceName(serviceConfig.getInterfaceName(),serviceConfig.getVersion(),serviceConfig.getGroup()), providerModel);
    }
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void doExportUrls(ServiceConfig serviceConfig) {
        List<URL> registryURLs = DubboConfigUtil.loadRegistries(true,serviceConfig);
        for (ProtocolConfig protocolConfig : serviceConfig.getProtocols()) {
            doExportUrlsFor1Protocol(serviceConfig,protocolConfig, registryURLs);
        }
    }

    private void doExportUrlsFor1Protocol(ServiceConfig serviceConfig,ProtocolConfig protocolConfig, List<URL> registryURLs) {
        String name = protocolConfig.getName();
        if (name == null || name.length() == 0) {
            name = "dubbo";
        }

        Map<String, String> map = new HashMap<String, String>();
        map.put(Constants.SIDE_KEY, Constants.PROVIDER_SIDE);
        map.put(Constants.DUBBO_VERSION_KEY, Version.getProtocolVersion());
        map.put(Constants.TIMESTAMP_KEY, String.valueOf(System.currentTimeMillis()));
        if (ConfigUtils.getPid() > 0) {
            map.put(Constants.PID_KEY, String.valueOf(ConfigUtils.getPid()));
        }
        DubboConfigUtil.appendParameters(map, serviceConfig.getApplication());
        DubboConfigUtil.appendParameters(map, serviceConfig.getModule());
        DubboConfigUtil.appendParameters(map, serviceConfig.getApplication().getProviderConfig(), Constants.DEFAULT_KEY);
        DubboConfigUtil.appendParameters(map, protocolConfig);
        DubboConfigUtil.appendParameters(map, serviceConfig);
        List<MethodConfig> methodConfigList = serviceConfig.getMethods();
        if (methodConfigList != null && !methodConfigList.isEmpty()) {
            for (MethodConfig method : methodConfigList) {
                DubboConfigUtil.appendParameters(map, method, method.getName());
                String retryKey = method.getName() + ".retry";
                if (map.containsKey(retryKey)) {
                    String retryValue = map.remove(retryKey);
                    if ("false".equals(retryValue)) {
                        map.put(method.getName() + ".retries", "0");
                    }
                }
                List<ArgumentConfig> arguments = method.getArguments();
                if (arguments != null && !arguments.isEmpty()) {
                    for (ArgumentConfig argument : arguments) {
                        // convert argument type
                        if (argument.getType() != null && argument.getType().length() > 0) {
                            Method[] methods = serviceConfig.getInterfaceClass().getMethods();
                            // visit all methods
                            if (methods != null && methods.length > 0) {
                                for (int i = 0; i < methods.length; i++) {
                                    String methodName = methods[i].getName();
                                    // target the method, and get its signature
                                    if (methodName.equals(method.getName())) {
                                        Class<?>[] argtypes = methods[i].getParameterTypes();
                                        // one callback in the method
                                        if (argument.getIndex() != -1) {
                                            if (argtypes[argument.getIndex()].getName().equals(argument.getType())) {
                                                DubboConfigUtil.appendParameters(map, argument, method.getName() + "." + argument.getIndex());
                                            } else {
                                                throw new IllegalArgumentException("argument config error : the index attribute and type attribute not match :index :" + argument.getIndex() + ", type:" + argument.getType());
                                            }
                                        } else {
                                            // multiple callbacks in the method
                                            for (int j = 0; j < argtypes.length; j++) {
                                                Class<?> argclazz = argtypes[j];
                                                if (argclazz.getName().equals(argument.getType())) {
                                                    DubboConfigUtil.appendParameters(map, argument, method.getName() + "." + j);
                                                    if (argument.getIndex() != -1 && argument.getIndex() != j) {
                                                        throw new IllegalArgumentException("argument config error : the index attribute and type attribute not match :index :" + argument.getIndex() + ", type:" + argument.getType());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } else if (argument.getIndex() != -1) {
                            DubboConfigUtil.appendParameters(map, argument, method.getName() + "." + argument.getIndex());
                        } else {
                            throw new IllegalArgumentException("argument config must set index or type attribute.eg: <dubbo:argument index='0' .../> or <dubbo:argument type=xxx .../>");
                        }

                    }
                }
            } // end of methods for
        }

        if (ProtocolUtils.isGeneric(serviceConfig.getGeneric())) {
            map.put(Constants.GENERIC_KEY, serviceConfig.getGeneric());
            map.put(Constants.METHODS_KEY, Constants.ANY_VALUE);
        } else {
            String revision = Version.getVersion(serviceConfig.getInterfaceClass(), serviceConfig.getVersion());
            if (revision != null && revision.length() > 0) {
                map.put("revision", revision);
            }

            String[] methods = Wrapper.getWrapper(serviceConfig.getInterfaceClass()).getMethodNames();
            if (methods.length == 0) {
                logger.warn("NO method found in service interface " + serviceConfig.getInterfaceClass().getName());
                map.put(Constants.METHODS_KEY, Constants.ANY_VALUE);
            } else {
                map.put(Constants.METHODS_KEY, StringUtils.join(new HashSet<String>(Arrays.asList(methods)), ","));
            }
        }
        if (!ConfigUtils.isEmpty(serviceConfig.getToken())) {
            if (ConfigUtils.isDefault(serviceConfig.getToken())) {
                map.put(Constants.TOKEN_KEY, UUID.randomUUID().toString());
            } else {
                map.put(Constants.TOKEN_KEY, serviceConfig.getToken());
            }
        }
        if (Constants.LOCAL_PROTOCOL.equals(protocolConfig.getName())) {
            protocolConfig.setRegister(false);
            map.put("notify", "false");
        }
        // export service
        String contextPath = protocolConfig.getContextpath();
        if ((contextPath == null || contextPath.length() == 0) && serviceConfig.getApplication().getProviderConfig() != null) {
            contextPath = serviceConfig.getApplication().getProviderConfig().getContextpath();
        }

        String host = this.findConfigedHosts(serviceConfig,protocolConfig, registryURLs, map);
        Integer port = this.findConfigedPorts(serviceConfig,protocolConfig, name, map);
        URL url = new URL(name, host, port, (contextPath == null || contextPath.length() == 0 ? "" : contextPath + "/") + serviceConfig.getPath(), map);
        if (ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class)
                .hasExtension(url.getProtocol())) {
            url = ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class)
                    .getExtension(url.getProtocol()).getConfigurator(url).configure(url);
        }

        String scope = url.getParameter(Constants.SCOPE_KEY);
        // don't export when none is configured
        if (!Constants.SCOPE_NONE.equalsIgnoreCase(scope)) {

            // export to local if the config is not remote (export to remote only when config is remote)
            if (!Constants.SCOPE_REMOTE.equalsIgnoreCase(scope)) {
                exportLocal(url,serviceConfig);
            }
            // export to remote if the config is not local (export to local only when config is local)
            if (!Constants.SCOPE_LOCAL.equalsIgnoreCase(scope)) {
                if (logger.isInfoEnabled()) {
                    logger.info("Export dubbo service " + serviceConfig.getInterfaceClass().getName() + " to url " + url);
                }
                if (registryURLs != null && !registryURLs.isEmpty()) {
                    for (URL registryURL : registryURLs) {
                        url = url.addParameterIfAbsent(Constants.DYNAMIC_KEY, registryURL.getParameter(Constants.DYNAMIC_KEY));
                        URL monitorUrl = DubboConfigUtil.loadMonitor(registryURL,serviceConfig.getMonitor(),serviceConfig.getApplication());
                        if (monitorUrl != null) {
                            url = url.addParameterAndEncoded(Constants.MONITOR_KEY, monitorUrl.toFullString());
                        }
                        if (logger.isInfoEnabled()) {
                            logger.info("Register dubbo service " + serviceConfig.getInterfaceClass().getName() + " url " + url + " to registry " + registryURL);
                        }

                        // For providers, this is used to enable custom proxy to generate invoker
                        String proxy = url.getParameter(Constants.PROXY_KEY);
                        if (StringUtils.isNotEmpty(proxy)) {
                            registryURL = registryURL.addParameter(Constants.PROXY_KEY, proxy);
                        }

                        Invoker<?> invoker = proxyFactory.getInvoker(serviceConfig.getRef(),  serviceConfig.getInterfaceClass(), registryURL.addParameterAndEncoded(Constants.EXPORT_KEY, url.toFullString()));
                        DelegateProviderMetaDataInvoker wrapperInvoker = new DelegateProviderMetaDataInvoker(invoker, serviceConfig);

                        Exporter<?> exporter = protocol.export(wrapperInvoker);
                        serviceConfig.getExporters().add(exporter);
                    }
                } else {
                    Invoker<?> invoker = proxyFactory.getInvoker(serviceConfig.getRef(), serviceConfig.getInterfaceClass(), url);
                    DelegateProviderMetaDataInvoker wrapperInvoker = new DelegateProviderMetaDataInvoker(invoker, serviceConfig);
                    Exporter<?> exporter = protocol.export(wrapperInvoker);
                    serviceConfig.getExporters().add(exporter);
                }
            }
        }
        serviceConfig.getUrls().add(url);
    }
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void exportLocal(URL url,ServiceConfig serviceConfig) {
        if (!Constants.LOCAL_PROTOCOL.equalsIgnoreCase(url.getProtocol())) {
            URL local = URL.valueOf(url.toFullString())
                    .setProtocol(Constants.LOCAL_PROTOCOL)
                    .setHost(LOCALHOST)
                    .setPort(0);
            Exporter<?> exporter = protocol.export(
                    proxyFactory.getInvoker(serviceConfig.getRef(), serviceConfig.getInterfaceClass(), local));
            serviceConfig.getExporters().add(exporter);
            logger.info("Export dubbo service " + serviceConfig.getInterfaceClass().getName() + " to local registry");
        }
    }

    private String getValueFromConfig(ProtocolConfig protocolConfig, String key) {
        String protocolPrefix = protocolConfig.getName().toUpperCase() + "_";
        String port = ConfigUtils.getSystemProperty(protocolPrefix + key);
        if (port == null || port.length() == 0) {
            port = ConfigUtils.getSystemProperty(key);
        }
        return port;
    }
    /**
     * Register & bind IP address for service provider, can be configured separately.
     * Configuration priority: environment variables -> java system properties -> host property in config file ->
     * /etc/hosts -> default network address -> first available network address
     *
     * @param protocolConfig
     * @param registryURLs
     * @param map
     * @return
     */
    private String findConfigedHosts(ServiceConfig serviceConfig,ProtocolConfig protocolConfig, List<URL> registryURLs, Map<String, String> map) {
        boolean anyhost = false;

        String hostToBind = getValueFromConfig(protocolConfig, Constants.DUBBO_IP_TO_BIND);
        if (hostToBind != null && hostToBind.length() > 0 && isInvalidLocalHost(hostToBind)) {
            throw new IllegalArgumentException("Specified invalid bind ip from property:" + Constants.DUBBO_IP_TO_BIND + ", value:" + hostToBind);
        }

        // if bind ip is not found in environment, keep looking up
        if (hostToBind == null || hostToBind.length() == 0) {
            hostToBind = protocolConfig.getHost();
            if (serviceConfig.getApplication().getProviderConfig() != null && (hostToBind == null || hostToBind.length() == 0)) {
                hostToBind = serviceConfig.getApplication().getProviderConfig().getHost();
            }
            if (isInvalidLocalHost(hostToBind)) {
                anyhost = true;
                try {
                    hostToBind = InetAddress.getLocalHost().getHostAddress();
                } catch (UnknownHostException e) {
                    logger.warn(e.getMessage(), e);
                }
                if (isInvalidLocalHost(hostToBind)) {
                    if (registryURLs != null && !registryURLs.isEmpty()) {
                        for (URL registryURL : registryURLs) {
                            if (Constants.MULTICAST.equalsIgnoreCase(registryURL.getParameter("registry"))) {
                                // skip multicast registry since we cannot connect to it via Socket
                                continue;
                            }
                            try {
                                Socket socket = new Socket();
                                try {
                                    SocketAddress addr = new InetSocketAddress(registryURL.getHost(), registryURL.getPort());
                                    socket.connect(addr, 1000);
                                    hostToBind = socket.getLocalAddress().getHostAddress();
                                    break;
                                } finally {
                                    try {
                                        socket.close();
                                    } catch (Throwable e) {
                                    }
                                }
                            } catch (Exception e) {
                                logger.warn(e.getMessage(), e);
                            }
                        }
                    }
                    if (isInvalidLocalHost(hostToBind)) {
                        hostToBind = getLocalHost();
                    }
                }
            }
        }

        map.put(Constants.BIND_IP_KEY, hostToBind);

        // registry ip is not used for bind ip by default
        String hostToRegistry = getValueFromConfig(protocolConfig, Constants.DUBBO_IP_TO_REGISTRY);
        if (hostToRegistry != null && hostToRegistry.length() > 0 && isInvalidLocalHost(hostToRegistry)) {
            throw new IllegalArgumentException("Specified invalid registry ip from property:" + Constants.DUBBO_IP_TO_REGISTRY + ", value:" + hostToRegistry);
        } else if (hostToRegistry == null || hostToRegistry.length() == 0) {
            // bind ip is used as registry ip by default
            hostToRegistry = hostToBind;
        }

        map.put(Constants.ANYHOST_KEY, String.valueOf(anyhost));

        return hostToRegistry;
    }
    private Integer parsePort(String configPort) {
        Integer port = null;
        if (configPort != null && configPort.length() > 0) {
            try {
                Integer intPort = Integer.parseInt(configPort);
                if (isInvalidPort(intPort)) {
                    throw new IllegalArgumentException("Specified invalid port from env value:" + configPort);
                }
                port = intPort;
            } catch (Exception e) {
                throw new IllegalArgumentException("Specified invalid port from env value:" + configPort);
            }
        }
        return port;
    }
    private static final Map<String, Integer> RANDOM_PORT_MAP = new HashMap<String, Integer>();

    private static Integer getRandomPort(String protocol) {
        protocol = protocol.toLowerCase();
        if (RANDOM_PORT_MAP.containsKey(protocol)) {
            return RANDOM_PORT_MAP.get(protocol);
        }
        return Integer.MIN_VALUE;
    }
    private static void putRandomPort(String protocol, Integer port) {
        protocol = protocol.toLowerCase();
        if (!RANDOM_PORT_MAP.containsKey(protocol)) {
            RANDOM_PORT_MAP.put(protocol, port);
            logger.warn("Use random available port(" + port + ") for protocol " + protocol);
        }
    }
    /**
     * Register port and bind port for the provider, can be configured separately
     * Configuration priority: environment variable -> java system properties -> port property in protocol config file
     * -> protocol default port
     *
     * @param protocolConfig
     * @param name
     * @return
     */
    private Integer findConfigedPorts(ServiceConfig serviceConfig,ProtocolConfig protocolConfig, String name, Map<String, String> map) {
        Integer portToBind = null;

        // parse bind port from environment
        String port = getValueFromConfig(protocolConfig, Constants.DUBBO_PORT_TO_BIND);
        portToBind = parsePort(port);

        // if there's no bind port found from environment, keep looking up.
        if (portToBind == null) {
            portToBind = protocolConfig.getPort();
            if (serviceConfig.getApplication().getProviderConfig() != null && (portToBind == null || portToBind == 0)) {
                portToBind = serviceConfig.getApplication().getProviderConfig().getPort();
            }
            final int defaultPort = ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(name).getDefaultPort();
            if (portToBind == null || portToBind == 0) {
                portToBind = defaultPort;
            }
            if (portToBind == null || portToBind <= 0) {
                portToBind = getRandomPort(name);
                if (portToBind == null || portToBind < 0) {
                    portToBind = getAvailablePort(defaultPort);
                    putRandomPort(name, portToBind);
                }
            }
        }

        // save bind port, used as url's key later
        map.put(Constants.BIND_PORT_KEY, String.valueOf(portToBind));
        // registry port, not used as bind port by default
        String portToRegistryStr = getValueFromConfig(protocolConfig, Constants.DUBBO_PORT_TO_REGISTRY);
        Integer portToRegistry = parsePort(portToRegistryStr);
        if (portToRegistry == null) {
            portToRegistry = portToBind;
        }
        return portToRegistry;
    }

    private void checkRef(ServiceConfig serviceConfig) {
        // reference should not be null, and is the implementation of the given interface
        if (serviceConfig.getRef() == null) {
            throw new IllegalStateException("ref not allow null!");
        }
        if (!serviceConfig.getInterfaceClass().isInstance(serviceConfig.getRef())) {
            throw new IllegalStateException("The class "
                    + serviceConfig.getRef().getClass().getName() + " unimplemented interface "
                    + serviceConfig.getInterfaceClass() + "!");
        }
    }

    public synchronized void export(ServiceConfig serviceConfig) {
        checkApplicationConfig(serviceConfig);
        //fixme  export参数应该直接引用ProviderConfig
        Boolean export = serviceConfig.getApplication().getProviderConfig().getExport();
        Integer delay = serviceConfig.getDelay();
        if (delay== null) {
            delay = serviceConfig.getApplication().getProviderConfig().getDelay();
        }
        if (export != null && !export) {
            return;
        }
        if (delay != null && delay > 0) {
            delayExportExecutor.schedule(() -> doExport(serviceConfig), delay, TimeUnit.MILLISECONDS);
        } else {
            doExport(serviceConfig);
        }
    }
}
