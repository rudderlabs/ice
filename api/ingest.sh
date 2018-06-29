#! /bin/bash

HOST=localhost:9200
INDEX=$1
BUCKET=$2

if [ $# -ne 2 ]; then
    echo "Wrong number of arguments"
    echo "Usage: $0 index bucket"
    exit 1
fi

echo Build index $INDEX from bucket $BUCKET

echo Delete existing index
curl -XDELETE $HOST/$INDEX; echo

echo Create new index
curl -XPUT -H "Content-Type: application/json" $HOST/$INDEX --data-binary '
{
    "settings": {
        "index": {
            "number_of_shards": 5,
            "number_of_replicas": 2
        }
    }
}'; echo

echo Add mapping for doc
curl -XPUT -H "Content-Type: application/json" $HOST/$INDEX/_mapping/doc --data-binary '
{
    "properties": {
        "hour": {
            "type": "date"
        },
        "accountId": {
            "type": "keyword"
        },
        "account": {
            "type": "keyword"
        },
        "region": {
            "type": "keyword"
        },
        "zone": {
            "type": "keyword"
        },
        "product": {
            "type": "keyword"
        },
        "operation": {
            "type": "keyword"
        },
        "usageType": {
            "type": "keyword"
        },
        "cost": {
            "type": "float"
        },
        "usage": {
            "type": "float"
        },
        "instanceFamily": {
            "type": "keyword"
        },
        "normalizedUsage": {
            "type": "float"
        },
        "normalizedRates": {
            "properties": {
                "onDemand": {
                    "type": "float"
                },
                "oneYearStd": {
                    "properties": {
                        "noUpfrontHourly": {
                            "type": "float"
                        },
                        "partialUpfrontFixed": {
                            "type": "float"
                        },
                        "partialUpfrontHourly": {
                            "type": "float"
                        },
                        "allUpfrontFixed": {
                            "type": "float"
                        }
                    }
                },
                "oneYearConv": {
                    "properties": {
                        "noUpfrontHourly": {
                            "type": "float"
                        },
                        "partialUpfrontFixed": {
                            "type": "float"
                        },
                        "partialUpfrontHourly": {
                            "type": "float"
                        },
                        "allUpfrontFixed": {
                            "type": "float"
                        }
                    }
                },
                "threeYearStd": {
                    "properties": {
                        "noUpfrontHourly": {
                            "type": "float"
                        },
                        "partialUpfrontFixed": {
                            "type": "float"
                        },
                        "partialUpfrontHourly": {
                            "type": "float"
                        },
                        "allUpfrontFixed": {
                            "type": "float"
                        }
                    }
                },
                "threeYearConv": {
                    "properties": {
                        "noUpfrontHourly": {
                            "type": "float"
                        },
                        "partialUpfrontFixed": {
                            "type": "float"
                        },
                        "partialUpfrontHourly": {
                            "type": "float"
                        },
                        "allUpfrontFixed": {
                            "type": "float"
                        }
                    }
                }
            }
        }
    }
}'; echo

CONF="input { stdin { codec => json_lines } } output { elasticsearch { hosts => [\"$HOST\"] index => \"$INDEX\" } }"
echo "$CONF"

aws s3 cp s3://$BUCKET/$INDEX.json.gz .

gunzip -c $INDEX.json.gz | logstash -e "$CONF"

exit 0
