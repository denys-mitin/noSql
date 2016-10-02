# Rump Up "NoSQL"

* fill Cassandra tables from Java
* create a Java tool to replicate data to ElasticSearch
* get ElasticSearch data from Java

### Useful queries

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