package org.kafkablocks.net;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;
import org.kafkablocks.AppProperties;


/**
 * Настройки для работы по сети через прокси-сервер
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Validated
@ConfigurationProperties(prefix = "whswd.net.proxy")
public class ProxyProperties extends AppProperties {
    private String host;
    private int port;
    private String userName;
    private String password;


    public boolean isEnabled() {
        // если хост задан, то считаем, что использование прокси включено
        return !StringUtils.isEmpty(host);
    }

    public boolean areCredentialsSet() {
        return !StringUtils.isEmpty(userName);
    }

    @Override
    protected void init() {
        if (!isEnabled())
            return;

        Assert.isTrue(1024 <= port && port <= 65535, "port must be positive");

        if (areCredentialsSet())
            Assert.isTrue(!StringUtils.isEmpty(password), "password must be set");
    }
}
