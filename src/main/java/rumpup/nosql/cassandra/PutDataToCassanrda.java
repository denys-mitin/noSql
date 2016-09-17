package rumpup.nosql.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/*
USE ramp_up;

CREATE TABLE "Visit" (
  id int PRIMARY KEY,
  "visitorId" int,
  "stationId" int,
  amount float,
  cost float
);

CREATE TABLE "Visitor" (
  id int PRIMARY KEY,
  "firstName" varchar,
  "lastName" varchar
);

CREATE TABLE "Station" (
  id int PRIMARY KEY,
  name varchar
);
*/

public class PutDataToCassanrda {

    public static void main(String[] args) {
        try (
                Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
                Session session = cluster.connect("ramp_up")) {
            ResultSet rs = session.execute("INSERT INTO \"Station\" (name) VALUES ('Station1')");
            session.execute("INSERT INTO \"Visitor\" (firstName, lastName) VALUES ('firstName', 'lastName')");
            //session.execute("INSERT INTO Visit (visitorId, stationId, amount, cost) VALUES ('firstName', 'lastName')");
        }
    }
}
