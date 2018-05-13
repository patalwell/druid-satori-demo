#!/bin/sh

QUERY=${1:-queries/topn_query.json}

curl -X POST 'http://pathdp2.field.hortonworks.com:8082/druid/v2/?pretty' -H 'content-type: application/json' -d@${QUERY}
