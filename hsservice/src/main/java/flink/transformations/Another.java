package flink.transformations;

/**
 *   Rebalance(重新平衡)
     均匀地重新平衡数据集的并行分区，以消除数据偏斜。
     重要：此操作会通过网络重新整理整个 DataSet。可能会花费大量时间。

     Hash-Partition (哈希分区) 详见mapPartition
     重要：此操作会通过网络重新整理整个 DataSet。可能会花费大量时间。

     Range-Partition（范围分区）
     重要：此操作需要在 DataSet 上额外传递一次以计算范围，通过网络对整个 DataSet 进行边界划分和改组。这会花费大量时间。

     Sort Partition（分区排序）
     以指定顺序对指定字段上的 DataSet 的所有分区进行本地排序。可以将字段指定为字段表达式或字段位置。
     可以通过链接 sortPartition() 调用在多个字段上对分区进行排序。

     First-n（前 n 个（任意）元素）
     返回数据集的前 n 个（任意）元素。First-n 可以应用于常规数据集，分组的数据集或分组排序的数据集。可以将分组键指定为键选择器功能或字段位置键。

 */
public class Another {
}
