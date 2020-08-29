package com.github.tm.glink.features;

import java.util.concurrent.ConcurrentLinkedQueue;

public  class ClassfiedGrids {
  private ConcurrentLinkedQueue<Long> confirmedIndexes;
  private ConcurrentLinkedQueue<Long> toCheckIndexes;
  public ClassfiedGrids() {
    confirmedIndexes = new ConcurrentLinkedQueue<>();
    toCheckIndexes = new ConcurrentLinkedQueue<>();
  }
  public void combine(ClassfiedGrids classfiedGrids) {
    confirmedIndexes.removeAll(classfiedGrids.confirmedIndexes);
    confirmedIndexes.addAll(classfiedGrids.confirmedIndexes);
    toCheckIndexes.removeAll(classfiedGrids.toCheckIndexes);
    toCheckIndexes.addAll(classfiedGrids.toCheckIndexes);
  }

  public int getGridsNum() {
    return confirmedIndexes.size() + toCheckIndexes.size();
  }

  public ConcurrentLinkedQueue<Long> getConfirmedIndexes() {
    return confirmedIndexes;
  }

  public ConcurrentLinkedQueue<Long> getToCheckIndexes() {
    return toCheckIndexes;
  }
  public void toCheckIndexesAdd(Long in) {
    toCheckIndexes.add(in);
  }

  public void confirmedIndexesAdd(Long in) {
    confirmedIndexes.add(in);
  }
}
