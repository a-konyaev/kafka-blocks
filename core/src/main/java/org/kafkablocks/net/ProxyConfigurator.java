package org.kafkablocks.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.*;
import java.util.Arrays;
import java.util.List;


/**
 * Компонент, который настраивает ProxySelector и Authenticator, если в конфиге заданы соотв. параметры
 *
 * Чтобы подключить данный компонент, нужно:
 * 1) импортировать его через @Import(ProxyConfigurator.class)
 * 2) явно выполнить инжект через @Autowired в компонент, которому нужна работа по сети через прокси
 * (это нужно для того, чтобы ProxyConfigurator был проинициализирован раньше, чем целевой компонент)
 *
 * todo: хорошо бы избавиться от необходимости явно инжектить ProxyConfigurator
 */
@Component
@EnableConfigurationProperties(ProxyProperties.class)
public class ProxyConfigurator {
    private final Logger logger = LoggerFactory.getLogger(ProxyConfigurator.class);
    private final ProxyProperties properties;
    private List<Proxy> proxies;


    @Autowired
    public ProxyConfigurator(ProxyProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    private void init() {
        if (!properties.isEnabled())
            return;

        InetSocketAddress address = new InetSocketAddress(properties.getHost(), properties.getPort());

        proxies = Arrays.asList(
                new Proxy(Proxy.Type.HTTP, address),
                new Proxy(Proxy.Type.SOCKS, address));

        ProxySelector.setDefault(new ProxySelector() {
            @Override
            public List<Proxy> select(URI uri) {
                return proxies;
            }

            @Override
            public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
                logger.warn("connectFailed: uri={}; sa={}; ex={}", uri, sa, ioe);
            }
        });

        if (properties.areCredentialsSet()) {
            Authenticator.setDefault(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(properties.getUserName(), properties.getPassword().toCharArray());
                }
            });
        }
    }
}
