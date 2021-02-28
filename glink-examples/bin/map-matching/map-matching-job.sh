flink run -c com.github.tm.glink.examples.mapmatching.MapMatchingJob glink-examples-1.0-SNAPSHOT \
--road-properties-path /home/liebing/Code/javaworkspace/glink/glink-examples/src/main/resources/mapmathcing/xiamen.properties \
--mode throughput \
--broker-list localhost:9092 \
--origin-topic map-matching-origin-throughput \
--result-topic map-matching-result-throughput