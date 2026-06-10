# TIBCO EMS Style JMS Management Console & Router

A Spring Boot application running an embedded ActiveMQ Artemis JMS broker, simulating a TIBCO EMS admin environment. It supports dynamic creation/deletion of queues and topics, dynamic topic-to-queue pairing (bridging) with SQL-92 selector filters, and a real-time responsive web dashboard.

---

## 🚀 Key Features

* **Embedded JMS Broker:** Runs ActiveMQ Artemis, isolated to VM transport and TCP port `61616`.
* **Dynamic Administration:** Create/delete Queues and Topics on the fly without rebooting the server.
* **Topic-to-Queue Bridging:** Pair Topics to Queues dynamically. Messages published to a topic are automatically duplicated and routed to all paired queues.
* **SQL Selector Filtering:** Apply optional SQL-92 selector filters on bridges (e.g. `priority > 5 AND region = 'US'`) to conditionalize routing.
* **Client Connection Panel:** Exposes the broker's connection factory classes, JNDI context configurations, and URLs (`tcp://localhost:61616` and `vm://0`) in the UI console.
* **Message Lab:** Send test messages with custom headers/properties, browse queues without consuming, or consume/acknowledge messages.
* **Live Topology Network:** A responsive SVG diagram illustrating topics, queues, and active bridges. Real-time messages trigger dot-packet animations flowing down the bridge lines.
* **Live SSE Telemetry:** Activity events (published, bridged, consumed) stream directly into the live log panel.

---

## 🛠️ Project Structure

```
c:\Users\DELL\work\JMSWithTopics
├── pom.xml                        # Maven configuration
├── mvnw / mvnw.cmd / .mvn/        # Maven wrapper for local compilation
├── walkthrough.md                 # Detailed walkthrough with screenshots & videos
├── task.md                        # Checklist of completed features
├── data/
│   └── ems-config.json            # Persistence file storing destinations & bridges
├── docs/
│   └── assets/                    # Screenshots and verification video files
└── src/
    └── main/
        ├── java/com/ems/jmsservice/
        │   ├── JmsServiceApplication.java    # Main Entrypoint
        │   ├── config/
        │   │   └── EmbeddedJmsConfig.java    # Embedded Artemis config with TCP Acceptor
        │   ├── model/
        │   │   ├── Destination.java          # Destination model (Queue/Topic)
        │   │   ├── JmsBridge.java            # Bridge pairing model (including SQL Selector)
        │   │   └── MessageLog.java           # Message history item model
        │   ├── repository/
        │   │   └── EmsConfigRepository.java  # Config persistence repository
        │   └── service/
        │       ├── JmsDestinationService.java# Dynamic destinations, browser, publisher service
        │       ├── JmsBridgeService.java     # Spawns dynamic message listener container bridges
        │       └── EmsEventBroadcaster.java  # Real-time SSE emitter manager
        └── resources/
            ├── application.properties        # Application configs
            └── static/
                ├── index.html                # Premium UI single-page dashboard
                ├── css/styles.css            # Custom glassmorphism stylesheet
                └── js/app.js                 # Event handlers and SVG animation scripts
```

---

## 💻 Getting Started

### Prerequisites
- Java 21 JDK (or Java 17+)

### 1. Build and Package
Build the Spring Boot jar file using the Maven wrapper:
```powershell
.\mvnw.cmd clean package -DskipTests
```

### 2. Start the Server
Run the packaged Spring Boot executable:
```powershell
java -jar target\jms-service-0.0.1-SNAPSHOT.jar
```

### 3. Open the Console Dashboard
Open your browser and navigate to:
```
http://localhost:8080/
```

### 4. Connect External JMS Clients
To connect external JMS clients (such as standard JMS publishers or subscribers) to this broker, use the following configuration parameters:
- **JMS Provider URL:** `tcp://localhost:61616` (or `vm://0` if connecting from within the same JVM)
- **Connection Factory Class:** `org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory`
- **Initial Context Factory:** `org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory`
- **Credentials:** Anonymous / No authentication required

---

## 📸 Media & Walkthroughs
For step-by-step verification, visual walkthroughs, screenshots of the dynamic bridge network, and video recordings demonstrating live packet animations, please check the local file:
👉 **[walkthrough.md](./walkthrough.md)**
