package dev.matchbook.gateway;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import quickfix.ApplicationAdapter;
import quickfix.DefaultMessageFactory;
import quickfix.FieldNotFound;
import quickfix.MemoryStoreFactory;
import quickfix.Message;
import quickfix.ScreenLogFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;
import quickfix.field.ClOrdID;
import quickfix.field.ExecType;
import quickfix.field.MsgType;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.OrigClOrdID;
import quickfix.field.Password;
import quickfix.field.Price;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.Text;
import quickfix.field.TransactTime;
import quickfix.fix44.NewOrderSingle;
import quickfix.fix44.OrderCancelRequest;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** A real QuickFIX/J initiator against the gateway's FIX 4.4 acceptor, backed by Redis and a real engine. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS) // the Spring context (and acceptor) starts before @BeforeAll
class FixGatewayIT {

    static final int FIX_PORT = freePort();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        TestEnvironment.register(registry, FIX_PORT);
    }

    /** A FIX client: logs on with the given API key and collects application messages. */
    static final class Client extends ApplicationAdapter {
        final String apiKey;
        final BlockingQueue<Message> received = new LinkedBlockingQueue<>();
        final BlockingQueue<String> logouts = new LinkedBlockingQueue<>();
        volatile boolean loggedOn;
        SocketInitiator initiator;
        SessionID session;

        Client(String senderCompId, String apiKey) throws Exception {
            this.apiKey = apiKey;
            SessionSettings settings = new SessionSettings();
            settings.setString("ConnectionType", "initiator");
            settings.setString("SocketConnectHost", "localhost");
            settings.setString("SocketConnectPort", Integer.toString(FIX_PORT));
            settings.setString("HeartBtInt", "30");
            settings.setString("ReconnectInterval", "60");
            settings.setString("NonStopSession", "Y");
            settings.setString("ResetOnLogon", "Y");
            settings.setString("UseDataDictionary", "Y");
            settings.setString("DataDictionary", "FIX44.xml");
            session = new SessionID("FIX.4.4", senderCompId, "MATCHBOOK");
            settings.setString(session, "BeginString", "FIX.4.4");
            initiator = new SocketInitiator(this, new MemoryStoreFactory(), settings, new ScreenLogFactory(false, false, false),
                    new DefaultMessageFactory());
            initiator.start();
        }

        @Override
        public void toAdmin(Message message, SessionID sessionId) {
            try {
                if (MsgType.LOGON.equals(message.getHeader().getString(MsgType.FIELD))) {
                    message.setString(Password.FIELD, apiKey);
                }
            } catch (FieldNotFound e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void fromAdmin(Message message, SessionID sessionId) throws FieldNotFound {
            if (MsgType.LOGOUT.equals(message.getHeader().getString(MsgType.FIELD))) {
                logouts.add(message.isSetField(Text.FIELD) ? message.getString(Text.FIELD) : "");
            }
        }

        @Override
        public void onLogon(SessionID sessionId) {
            loggedOn = true;
        }

        @Override
        public void fromApp(Message message, SessionID sessionId) {
            received.add(message);
        }

        void awaitLogon() throws InterruptedException {
            long deadline = System.currentTimeMillis() + 15_000;
            while (!loggedOn && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertThat(loggedOn).as("logged on").isTrue();
        }

        void send(Message message) {
            assertThat(Session.lookupSession(session).send(message)).isTrue();
        }

        /** Next application message satisfying the predicate (others are skipped). */
        Message next(java.util.function.Predicate<Message> matching) throws Exception {
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                Message m = received.poll(500, TimeUnit.MILLISECONDS);
                if (m != null && matching.test(m)) {
                    return m;
                }
            }
            throw new AssertionError("no matching FIX message");
        }

        void stop() {
            initiator.stop(true);
        }
    }

    Client trader1;
    Client trader2;

    @BeforeAll
    void connect() throws Exception {
        trader1 = new Client("TRADER1", TestEnvironment.KEY_1);
        trader2 = new Client("TRADER2", TestEnvironment.KEY_2);
        trader1.awaitLogon();
        trader2.awaitLogon();
    }

    @AfterAll
    void disconnect() {
        trader1.stop();
        trader2.stop();
    }

    static NewOrderSingle limit(String clOrdId, String symbol, char side, String price, int qty) {
        NewOrderSingle m = new NewOrderSingle(new ClOrdID(clOrdId), new Side(side), new TransactTime(LocalDateTime.now()),
                new OrdType(OrdType.LIMIT));
        m.set(new Symbol(symbol));
        m.setString(OrderQty.FIELD, Integer.toString(qty));
        m.setString(Price.FIELD, price);
        return m;
    }

    static java.util.function.Predicate<Message> execFor(String clOrdId, char execType) {
        return m -> {
            try {
                return MsgType.EXECUTION_REPORT.equals(m.getHeader().getString(MsgType.FIELD))
                        && clOrdId.equals(m.getString(ClOrdID.FIELD)) && m.getChar(ExecType.FIELD) == execType;
            } catch (FieldNotFound e) {
                return false;
            }
        };
    }

    @Test
    void restingOrderIsAcknowledgedAndItsFillArrivesAsynchronously() throws Exception {
        String sell = "fix-" + UUID.randomUUID();
        trader1.send(limit(sell, "TSLA", Side.SELL, "300.25", 40));
        Message ack = trader1.next(execFor(sell, ExecType.NEW));
        assertThat(ack.getChar(OrdStatus.FIELD)).isEqualTo(OrdStatus.NEW);
        assertThat(ack.getString(Price.FIELD)).isEqualTo("300.25");

        String buy = "fix-" + UUID.randomUUID();
        trader2.send(limit(buy, "TSLA", Side.BUY, "300.30", 25));
        Message takerFill = trader2.next(execFor(buy, ExecType.TRADE));
        assertThat(takerFill.getChar(OrdStatus.FIELD)).isEqualTo(OrdStatus.FILLED);
        assertThat(takerFill.getString(quickfix.field.LastPx.FIELD)).isEqualTo("300.25");
        assertThat(takerFill.getString(quickfix.field.AvgPx.FIELD)).isEqualTo("300.25");

        Message makerFill = trader1.next(execFor(sell, ExecType.TRADE));
        assertThat(makerFill.getChar(OrdStatus.FIELD)).isEqualTo(OrdStatus.PARTIALLY_FILLED);
        assertThat(makerFill.getDouble(quickfix.field.LeavesQty.FIELD)).isEqualTo(15.0);
    }

    @Test
    void cancelUsesTheEngineOrderIdAndReportsTheOriginalClOrdId() throws Exception {
        String buy = "fix-" + UUID.randomUUID();
        trader1.send(limit(buy, "TSLA", Side.BUY, "295.00", 10)); // within the fat-finger band of any earlier TSLA trade
        String orderId = trader1.next(execFor(buy, ExecType.NEW)).getString(OrderID.FIELD);

        String cancelId = "fix-" + UUID.randomUUID();
        OrderCancelRequest cancel = new OrderCancelRequest(new OrigClOrdID(buy), new ClOrdID(cancelId), new Side(Side.BUY),
                new TransactTime(LocalDateTime.now()));
        cancel.set(new Symbol("TSLA"));
        cancel.set(new OrderID(orderId));
        trader1.send(cancel);
        Message canceled = trader1.next(execFor(cancelId, ExecType.CANCELED));
        assertThat(canceled.getString(OrigClOrdID.FIELD)).isEqualTo(buy);
        assertThat(canceled.getChar(OrdStatus.FIELD)).isEqualTo(OrdStatus.CANCELED);

        String again = "fix-" + UUID.randomUUID();
        OrderCancelRequest second = new OrderCancelRequest(new OrigClOrdID(buy), new ClOrdID(again), new Side(Side.BUY),
                new TransactTime(LocalDateTime.now()));
        second.set(new Symbol("TSLA"));
        second.set(new OrderID(orderId));
        trader1.send(second);
        Message rejected = trader1.next(m -> {
            try {
                return MsgType.ORDER_CANCEL_REJECT.equals(m.getHeader().getString(MsgType.FIELD))
                        && again.equals(m.getString(ClOrdID.FIELD));
            } catch (FieldNotFound e) {
                return false;
            }
        });
        assertThat(rejected.getString(Text.FIELD)).isEqualTo("UNKNOWN_ORDER");
    }

    @Test
    void gatewayRejectsAndDuplicatesAreAnsweredDirectly() throws Exception {
        String big = "fix-" + UUID.randomUUID();
        trader1.send(limit(big, "TSLA", Side.BUY, "200.00", 200_000));
        Message tooBig = trader1.next(execFor(big, ExecType.REJECTED));
        assertThat(tooBig.getString(Text.FIELD)).isEqualTo("RISK_MAX_ORDER_QUANTITY");

        String dup = "fix-" + UUID.randomUUID();
        trader1.send(limit(dup, "TSLA", Side.BUY, "296.00", 1));
        trader1.next(execFor(dup, ExecType.NEW));
        trader1.send(limit(dup, "TSLA", Side.BUY, "296.00", 1));
        Message duplicate = trader1.next(execFor(dup, ExecType.REJECTED));
        assertThat(duplicate.getInt(quickfix.field.OrdRejReason.FIELD)).isEqualTo(quickfix.field.OrdRejReason.DUPLICATE_ORDER);
    }

    @Test
    void logonWithAWrongKeyIsRejected() throws Exception {
        Client intruder = new Client("INTRUDER", "not-a-key");
        try {
            String reason = intruder.logouts.poll(10, TimeUnit.SECONDS);
            assertThat(reason).isNotNull();
            assertThat(intruder.loggedOn).isFalse();
        } finally {
            intruder.stop();
        }
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
