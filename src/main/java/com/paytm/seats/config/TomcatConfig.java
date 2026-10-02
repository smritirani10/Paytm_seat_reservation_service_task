package com.paytm.seats.config;

import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

/**
 * Memory-bounded handling of a 20k-connection stampede.
 *
 * Every connection is accepted (max-connections in application.yml) and waits
 * in Tomcat's NIO poller, which costs only its socket buffers. Only a fixed
 * pool of worker threads processes requests at once, so the expensive
 * per-request state (header/response buffers, about 100 KB per request) is
 * bounded by the pool size, not by the number of clients. Small JSON requests
 * don't need Tomcat's default 8 KB socket buffers, so we use 4 KB.
 */
@Component
public class TomcatConfig implements WebServerFactoryCustomizer<TomcatServletWebServerFactory> {
    @Override
    public void customize(TomcatServletWebServerFactory factory) {
        factory.addConnectorCustomizers(connector -> {
            connector.setProperty("socket.appReadBufSize", "4096");
            connector.setProperty("socket.appWriteBufSize", "4096");
        });
    }
}
