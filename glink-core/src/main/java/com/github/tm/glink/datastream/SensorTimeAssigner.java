/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.tm.glink.datastream;

import com.github.tm.glink.grid.Tile;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Geometry;

/**
 * Assigns timestamps to SensorReadings based on their internal timestamp and
 * emits watermarks with five seconds slack.
 */
public class SensorTimeAssigner<T extends Geometry> extends BoundedOutOfOrdernessTimestampExtractor<T> {

    /**
     * Configures the extractor with 5 seconds out-of-order interval.
     */
    public SensorTimeAssigner() {
        super(Time.seconds(1));
    }

    /**
     * Extracts timestamp from SensorReading.
     *
     * @param r sensor reading
     * @return the timestamp of the sensor reading.
     */
    @Override
    public long extractTimestamp(T r) {
        Tuple2<Tile, Tuple> attributes = (Tuple2<Tile, Tuple>) r.getUserData();
        return attributes.f1.getField(2);
    }
}
