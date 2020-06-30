package kafkablocks.utils;

import lombok.SneakyThrows;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.HttpClients;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;

import javax.net.ssl.SSLContext;
import java.security.cert.X509Certificate;

public final class NetUtils {
    private NetUtils() {
    }

    private static ClientHttpRequestFactory createClientHttpRequestFactory(HttpClient httpClient) {
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
        requestFactory.setHttpClient(httpClient);
        return requestFactory;
    }

    /**
     * Получить фабрику, которая игнорирует невалидные сертификаты (не выполняет их верификацию)
     */
    public static ClientHttpRequestFactory getSkipSslCertVerificationRequestFactory() {
        return createClientHttpRequestFactory(getSkipSslCertVerificationHttpClient());
    }

    @SneakyThrows
    private static HttpClient getSkipSslCertVerificationHttpClient() {
        TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;

        SSLContext sslContext = org.apache.http.ssl.SSLContexts.custom()
                .loadTrustMaterial(null, acceptingTrustStrategy)
                .build();

        return HttpClients
                .custom()
                .setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext, new NoopHostnameVerifier()))
                .build();
    }
}
