

CREATE KEYSPACE ramp_up WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor' : 3};

USE ramp_up;

CREATE TABLE "Station" (
  id uuid PRIMARY KEY,
  name varchar
);

CREATE TABLE "Visitor" (
  id uuid PRIMARY KEY,
  "firstName" varchar,
  "lastName" varchar
);

CREATE TABLE "Visit" (
  id uuid PRIMARY KEY,
  "visitorId" uuid,
  "stationId" uuid,
  amount float,
  cost float
);