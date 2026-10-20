package dev.matchbook.gateway.fix;

import dev.matchbook.gateway.config.GatewayProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.LogFactory;
import quickfix.MemoryStoreFactory;
import quickfix.MessageStoreFactory;
import quickfix.SLF4JLogFactory;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.ThreadedSocketAcceptor;
import quickfix.mina.acceptor.DynamicAcceptorSessionProvider;

import java.net.InetSocketAddress;

/**
 * FIX 4.4 acceptor (QuickFIX/J) on {@code matchbook.fix.port}. Sessions are created dynamically for any
 * client SenderCompID; {@link FixApplication} authenticates the Logon and routes orders into the same
 * {@code OrderService} as gRPC and REST.
 *
 * <p>Message stores are in memory and sequence numbers reset on each logon ({@code ResetOnLogon}): the
 * gateway does not persist FIX sessions, so a reconnecting client resynchronises from its execution
 * reports rather than through FIX resend requests. See ADR-0010.
 */
@Component
public class FixGateway implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(FixGateway.class);

    private final GatewayProperties.Fix config;
    private final FixApplication application;
    private ThreadedSocketAcceptor acceptor;
    private volatile boolean running;

    public FixGateway(GatewayProperties properties, FixApplication application) {
        this.config = properties.fix();
        this.application = application;
    }

    @Override
    public void start() {
        if (!config.enabled()) {
            return;
        }
        try {
            SessionSettings settings = new SessionSettings();
            settings.setString("ConnectionType", "acceptor");
            settings.setString("SocketAcceptPort", Integer.toString(config.port()));
            settings.setString("NonStopSession", "Y");
            settings.setString("ResetOnLogon", "Y");
            settings.setString("UseDataDictionary", "Y");
            settings.setString("DataDictionary", "FIX44.xml");
            settings.setString("ValidateUserDefinedFields", "N");
            SessionID template = new SessionID("FIX.4.4", config.senderCompId(), "*");
            settings.setString(template, "AcceptorTemplate", "Y");

            MessageStoreFactory stores = new MemoryStoreFactory();
            LogFactory logs = new SLF4JLogFactory(settings);
            DefaultMessageFactory messages = new DefaultMessageFactory();
            acceptor = new ThreadedSocketAcceptor(application, stores, settings, logs, messages);
            acceptor.setSessionProvider(new InetSocketAddress("0.0.0.0", config.port()),
                    new DynamicAcceptorSessionProvider(settings, template, application, stores, logs, messages));
            acceptor.start();
            running = true;
            LOG.info("FIX 4.4 acceptor listening on port {} as {}", config.port(), config.senderCompId());
        } catch (ConfigError e) {
            throw new IllegalStateException("could not start the FIX acceptor", e);
        }
    }

    @Override
    public void stop() {
        if (acceptor != null) {
            acceptor.stop();
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
