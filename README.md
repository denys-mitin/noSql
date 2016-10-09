# Rump Up "NoSQL"

* fill Cassandra tables from Java
* create a Java tool to replicate data to ElasticSearch
* get ElasticSearch data from Java

### Useful queries

** Test data **

POST ramp_up/visit
{
  "id": "b7cd58de40eb41b4aa18f6b73887b35e",
  "station": {
    "id": "b47c145be4134ff8a7f76fe9750f7d73",
    "name": "Station1",
    "location": {
      "lat": 50,
      "lon": 34
    }
  },
  "visitor": {
    "id": "588528bacdbd4fc38d48ca914f2c271a",
    "firstName": "FirstName3",
    "lastName": "LastName3"
  },
  "amount": 0.2950827,
  "cost": 0.13086319,
  "time": 1475977670438
}

** Template with uuids, timestamps and geo fields **
```
PUT _template/ramp_up
{
  "template": "ramp_up",
  "mappings": {
    "visit": {
      "properties": {
        "station": {
          "properties": {
            "location": {
              "type": "geo_point"
            }
          }
        },
        "time": {
          "type": "date"
        }
      }
    }
  }
}
```
Note: ElasticSearch has no uuid type.

** Stations by total amount of petrol sold **
```
GET ramp_up/_search
{
  "size": 0,
  "aggs": {
    "by_sum_amount": {
      "terms": {
        "field": "station.name",
        "order": {
          "sum_amount": "desc"
        }
      },
      "aggs": {
        "sum_amount": {
          "sum": {
            "field": "amount"
          }
        }
      }
    }
  }
}
```

** Stations by count of visits **
```
GET ramp_up/_search
{
  "size": 0,
  "aggs": {
      "by_sum_amount": {
      "terms": {
        "field": "station.name",
        "order": {
          "count_visits": "desc"
        }
      },
      "aggs": {
        "count_visits": {
          "value_count": {
            "field": "station.name"
          }
        }
      }
    }
  }
}
```

** Top visitors by total amount paid **
```
GET ramp_up/_search
{
  "size": 0,
  "aggs": {
    "by_sum_amount": {
      "terms": {
        "field": "visitor.firstName",
        "order": {
          "sum_amount": "desc"
        }
      },
      "aggs": {
        "sum_amount": {
          "sum": {
            "field": "amount"
          }
        }
      }
    }
  }
}
```