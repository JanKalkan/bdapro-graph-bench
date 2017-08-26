package com.tu.bdap.utils;

import org.apache.flink.graph.Graph;

/**
 * Created by jan on 18.08.17.
 */
public interface GraphLoader {

    public Graph load(String path);

}
