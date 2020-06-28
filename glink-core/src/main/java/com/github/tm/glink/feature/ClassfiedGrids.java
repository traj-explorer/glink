package com.github.tm.glink.feature;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public  class ClassfiedGrids {
    public ConcurrentLinkedQueue<Long> confirmedIndexes;
    public ConcurrentLinkedQueue<Long> toCheckIndexes;
    public ClassfiedGrids(){
        confirmedIndexes = new ConcurrentLinkedQueue<>();
        toCheckIndexes = new ConcurrentLinkedQueue<>();
    }
    public void combine(ClassfiedGrids classfiedGrids){
        confirmedIndexes.removeAll(classfiedGrids.confirmedIndexes);
        confirmedIndexes.addAll(classfiedGrids.confirmedIndexes);
        toCheckIndexes.removeAll(classfiedGrids.toCheckIndexes);
        toCheckIndexes.addAll(classfiedGrids.toCheckIndexes);
    }

    public int getGridsNum(){
        return confirmedIndexes.size()+toCheckIndexes.size();
    }
}
