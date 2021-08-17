package com.google.common.collect;

import com.google.common.base.Function;

import java.util.concurrent.ConcurrentMap;

public class MigrateMap {

  @SuppressWarnings("deprecation")
  public static <K, V> ConcurrentMap<K, V> makeComputingMap(MapMaker maker,
      Function<? super K, ? extends V> computingFunction) {
    return maker.makeComputingMap(computingFunction);
  }

  @SuppressWarnings("deprecation")
  public static <K, V> ConcurrentMap<K, V> makeComputingMap(Function<? super K, ? extends V> computingFunction) {
    return new MapMaker().makeComputingMap(computingFunction);
  }

  public static <K, V> ConcurrentMap<K, V> makeMap() {
    return new MapMaker().makeMap();
  }
}
