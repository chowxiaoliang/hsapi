package spark.core.selfdefine;

import org.apache.spark.Partitioner;

/**
 * 自定义分区
 */
public class MyPartitioner extends Partitioner {

    private int numPartitions;

    public MyPartitioner(){}

    public MyPartitioner(int numPartitions){
        this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        return key.hashCode() % numPartitions;
    }
}
