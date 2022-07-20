package com.ebay.sojourner.rt.util;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;

public class Test {

  public static void main(String[] args) {
//
//    byte[] guidSetForAgentIp5 = null;
//    HllSketch guidSet;
//    if (guidSetForAgentIp5 == null) {
//      guidSet = new HllSketch(12, TgtHllType.HLL_8);
//    } else {
//      guidSet = HllSketch.heapify(guidSetForAgentIp5);
//    }
//    long[] guidList = {1, 2};
//    guidSet.update(guidList);
//    guidSetForAgentIp5 = guidSet.toCompactByteArray();
//
//
//    if (guidSetForAgentIp5 == null) {
//      guidSet = new HllSketch(12, TgtHllType.HLL_8);
//    } else {
//      guidSet = HllSketch.heapify(guidSetForAgentIp5);
//    }
//    long[] guidList2 = {1, 3};
//    guidSet.update(guidList2);
//
//    long[] guidList6 = {2, 2};
//    guidSet.update(guidList6);
//
//    System.out.println(guidSet.getEstimate());
//    guidSetForAgentIp5 = guidSet.toCompactByteArray();
//
//
//
//    byte[] guidSetForAgentIp6 = null;
//    HllSketch guidSet3;
//    if (guidSetForAgentIp6 == null) {
//      guidSet3 = new HllSketch(12, TgtHllType.HLL_8);
//    } else {
//      guidSet3 = HllSketch.heapify(guidSetForAgentIp6);
//    }
//    long[] guidList3 = {2, 2};
//    guidSet3.update(guidList3);
//    guidSetForAgentIp6 = guidSet3.toCompactByteArray();
//
//
//    if (guidSetForAgentIp6 == null) {
//      guidSet3 = new HllSketch(12, TgtHllType.HLL_8);
//    } else {
//      guidSet3 = HllSketch.heapify(guidSetForAgentIp6);
//    }
//    long[] guidList4 = {2, 3};
//    guidSet3.update(guidList4);
//
//    long[] guidList7 = {4, 3};
//    guidSet3.update(guidList7);
//    System.out.println(guidSet3.getEstimate());
//    guidSetForAgentIp6 = guidSet3.toCompactByteArray();
//
//
//
//    Union guidSet4= Union.heapify(guidSetForAgentIp5);
//    HllSketch guidSet5 = HllSketch.heapify(guidSetForAgentIp6);
//    guidSet4.update(guidSet5);
//
//
//    System.out.println(guidSet4.getEstimate());
//
//
//
//    HllSketch aa = new HllSketch(8, TgtHllType.HLL_8);
//    for (int i=1;i<10000;i++){
//      aa.update(i);
//    }
//    System.out.println(aa.toCompactByteArray().length);
//    System.out.println(aa.getEstimate());

    double max = Double.MIN_VALUE;

    for (int j = 0; j < 1000; j++) {
      Union union = new Union(12);
      Random random = new Random();
//      Set<Integer> num = new HashSet<>();
      for (int i = 1; i < 10000; i++) {
        int n = random.nextInt();
//        num.add(n);
        union.update(n);
      }
      //System.out.println(union.toCompactByteArray().length);
//      System.out.println(num.size());

      //System.out.println(union.getEstimate());
      double value = Math.abs(10000 - union.getEstimate()) / 10000;
      if (value > 0.02) {
        System.out.println("current value: " + value);
      }
      if (max < value) {
        max = value;
      }

    }

    System.out.println("max value: " + max);

    System.out.println("---------------------");


    max = Double.MIN_VALUE;
    for (int j = 0; j < 1000; j++) {
      Union union = new Union(12);
//      Set<Integer> num = new HashSet<>();
      for (int i = 1; i < 10000; i++) {
        union.update(i);
      }
      //System.out.println(union.toCompactByteArray().length);
//      System.out.println(num.size());

      //System.out.println(union.getEstimate());
      double value = Math.abs(10000 - union.getEstimate()) / 10000;
      if (value > 0.02) {
        System.out.println("current value: " + value);
      }
      if (max < value) {
        max = value;
      }
    }

    System.out.println("max value: " + max);
  }
}
