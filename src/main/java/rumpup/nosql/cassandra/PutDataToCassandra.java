package rumpup.nosql.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.util.Random;
import java.util.UUID;

public class PutDataToCassandra {

    public static final int STATIONS_COUNT = 4;

    public static final int VISITORS_COUNT = 10;

    public static final int VISITS_COUNT = 1000;

    public static void main(String[] args) {
        try (
                Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
                Session session = cluster.connect("ramp_up")) {
            session.execute("TRUNCATE \"Visit\"");
            session.execute("TRUNCATE \"Visitor\"");
            session.execute("TRUNCATE \"Station\"");
            UUID[] stationUUIDs = new UUID[STATIONS_COUNT];
            for (int i = 0; i < STATIONS_COUNT; i++) {
                stationUUIDs[i] = UUID.randomUUID();
                session.execute("INSERT INTO \"Station\" (id, name) VALUES (?, ?)",
                        stationUUIDs[i], "Station" + i);
            }
            UUID[] visitorUUIDs = new UUID[VISITORS_COUNT];
            for (int i = 0; i < VISITORS_COUNT; i++) {
                visitorUUIDs[i] = UUID.randomUUID();
                session.execute("INSERT INTO \"Visitor\" (id, \"firstName\", \"lastName\") VALUES (?, ?, ?)",
                        visitorUUIDs[i], "FirstName" + i, "LastName" + i);
            }
            Random r = new Random(System.currentTimeMillis());
            for (int i = 0; i < VISITS_COUNT; i++) {
                UUID stationId = stationUUIDs[r.nextInt(STATIONS_COUNT)];
                UUID visitorId = visitorUUIDs[r.nextInt(VISITORS_COUNT)];
                float amount = r.nextFloat();
                float cost = r.nextFloat();
                session.execute("INSERT INTO \"Visit\" (id, \"visitorId\", \"stationId\", amount, cost) VALUES (?, ?, ?, ?, ?)",
                        UUID.randomUUID(), visitorId, stationId, amount, cost);
            }
        }
        System.exit(0);
    }
}