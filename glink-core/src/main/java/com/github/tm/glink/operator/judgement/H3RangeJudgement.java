package com.github.tm.glink.operator.judgement;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import com.github.tm.glink.feature.ClassfiedGrids;
import com.github.tm.glink.feature.GeoObject;
import com.github.tm.glink.feature.Point;
import com.github.tm.glink.index.H3Index;
import com.sun.tools.corba.se.idl.toJavaPortable.Helper24;
import org.locationtech.jts.geom.*;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

/**
 * @author Yu Liebing
 */
public class H3RangeJudgement< T extends Point,U extends Geometry> extends RangeJudgementBase<T> {

    private List<U> queryGeometries = new ArrayList<>();
    private GeometryFactory geometryFactory = new GeometryFactory();
    private int res;
    private transient ValueState<List<Long>> confirmedIndexes;
    private transient ValueState<List<Long>>  toCheckIndexes;

    public H3RangeJudgement(U queryGeometry, int res) {
        queryGeometries.add(queryGeometry);
        this.res = res;
    }

    public H3RangeJudgement(Envelope queryWindow,int res) {
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(queryWindow.getMinX(), queryWindow.getMinY());
        coordinates[1] = new Coordinate(queryWindow.getMinX(), queryWindow.getMaxY());
        coordinates[2] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMaxY());
        coordinates[3] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMinY());
        coordinates[4] = coordinates[0];
        U queryGeometry = (U) geometryFactory.createPolygon(coordinates);
        queryGeometries.add(queryGeometry);
        this.res = res;
    }

    public H3RangeJudgement(int res, U... queryGeometries) {
        this.queryGeometries.addAll(Arrays.asList(queryGeometries));
        this.res = res;
    }

    public H3RangeJudgement(int res,List<U> queryGeometries) {
        this.queryGeometries.addAll(queryGeometries);
        this.res = res;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<List<Long>> ciDescipter = new ValueStateDescriptor<List<Long>>(
            "Confirmed Indexes",
            TypeInformation.of(new TypeHint<List<Long>>() {}));
        ValueStateDescriptor<List<Long>> tcDescipter = new ValueStateDescriptor<List<Long>>(
            "To Check Indexes",
            TypeInformation.of(new TypeHint<List<Long>>() {}));
        confirmedIndexes = getRuntimeContext().getState(ciDescipter);
        toCheckIndexes = getRuntimeContext().getState(tcDescipter);
    }

    @Override
    public boolean rangeFilter(T geoObject) throws IOException {
        if(confirmedIndexes.value()==null || toCheckIndexes.value()==null){
            H3Index h3Index = new H3Index(res);
            ClassfiedGrids classfiedGrids = new ClassfiedGrids();
            for(U queryGeometry:queryGeometries){
                classfiedGrids.combine(h3Index.getContainGrids(queryGeometry));
            }
            confirmedIndexes.update(new ArrayList<>(classfiedGrids.confirmedIndexes));
            toCheckIndexes.update(new ArrayList<>(classfiedGrids.toCheckIndexes));
        }
        H3Index h3Index = new H3Index(res);
        Long index = h3Index.getIndex(geoObject.getLat(),geoObject.getLng());
        for (U queryGeometry : queryGeometries) {
            if (confirmedIndexes.value().contains(index)) {
                return true;
            } else if (toCheckIndexes.value().contains(index)) {
                return queryGeometry.contains(geoObject.getGeometry(geometryFactory));
            } else
                return false;
        }
        return false;
    }
}
