# 时空数据流建模
时空对象快照: 某一时间窗口或时间片内的所有时空对象.

# 时空数据流空间关系模型
时空数据流空间关系模型是对每个时空对象快照进行空间关系建模.

## Index

Glink中，我们提供了基于格网、树的两种空间索引，分别对应了`GridIndex`与`SpatialTreeIndex`抽象类。格网索引用于对所有轨迹点进行初步的分离，树索建立于格网内部，适用于格网内轨迹点的快速查询。下面将分别介绍格网索引、树型索引。

### 格网索引

空间格网索引是二维的，给定一种格网索引与特定的缩放等级，可以将地理空间划分为多个区域，点状要素通过自身的经纬度信息，即可快速判断所处的格网单元。`index.GridIndex`抽象类为格网索引提供了全面的方法：

1. 用于获取点坐标为(`lat`,`lng`)的格网id。这里，获取到的格网索引的缩放等级由索引初始化时设置的的`res`属性确定。

   ```java
   Long getIndex(double lat, double lng)
   ```

2. 用于获取以(`lat`,`lng`)为中心，`2 * distance`为宽、高的矩形区域内的格网索引。

   ```java
   List<Long> getRangeIndex(double lat, double lng, double distance, boolean fullMode);
   ```

   > 待确定：fullMode 参数的含义
   >
   > H3Index暂未实现

3. 用于获取与`geoObject`边界部分相交的格网索引。

   ```java
   List<Long> getIntersectIndex(Geometry geoObject)
   ```

4. 用于获取索引为`index`的格网的边界要素

   ```java
   Geometry getGeoBoundary(long index)
   ```

   > H3Index与UGridIndex暂未实现。

5. 获取索引为`index`的子格网集/父格网。当未指定`res`时，将根据当前的`index`所处的分辨率判断子/父格网。当`res`给定时，将获取`index`在缩放等级为`res`下的子/父格网。

   ```java
   Long getParent(long index)
   Long getParent(long index, int res)
   List<Long> getChildren(long index)
   List<Long> getChildren(long index, int res)
   ```

   > H3Index与UGridIndex暂未实现。

6. 获取与几何`geometry` 相关的 `ClassfiedGrids`类型的索引集合。`ClassfiedGrids`内含对相关的格网id的两类分类：包含于`geometry`内部的格网索引、与`geometry`相交的格网索引，后续可用于范围查询。

   ```java
   ClassfiedGrids getRelatedGrids(Geometry geometry)
   ```

   > H3Index未实现

7. 获取索引为`index`的格网周边第`k`环的格网集合。

   ```java
   List<Long> kRing(long index, int k);
   ```

   > H3Index与UGridIndex暂未实现。

#### UGridIndex的getRangeIndex设计

`getRangeIndex`方法用于在DBSCAN中，快速、低冗余地获取在距离阈值$\epsilon$所有临近点对（Neighbor Pair）。

考虑如下的情况：实线矩形为各个格网的范围，虚线矩形为以A点为中心，$2\epsilon$为长宽的查询矩形$Q_R$。若将点仅分配至点所在的格网中，当该点与查询点在不同格网且处于查询内部时（点B、C、D），查询任务将会很困难。

由于Flink中各个Key之间的数据相互独立，那么需要特别设计一种数据传播机制，达到以下两个目的：

1. 将位于格网边缘的点分配到可能被查询到的格网，使它能够与相近的其他格网的点汇聚到同一个节点中，在节点内查询，构成临近点对。
2. 获取临近点对时应减少冗余。例如，不做任何处理的情况下，当A、B点相近且处于不同格网，以A为查询中心时将得到临近点对（A，B），而以B为查询中心将得到冗余的（B，A），如此将加重机器的负担。

为了达到这个目的，需要指定一个冗余距离$\epsilon$ （查询范围的一半），对于距格网边缘距离小于等于$\epsilon$的点进行如下图的数据传播：







### 树形索引



## Queries

GeoFlink中，针对轨迹点数据已实现了kNN、RangeQuery两类查询。

### KNN Query

1. 输入：某一查询点$Q$、轨迹点数据流$S$、时间窗口范围$W$、待查近邻点个数$k$。
2. 作用：在窗口$W$结束时，以轨迹点数据流$S$中落入$W$的数据子集为查询范围，以查询点$Q$为中心，进行kNN查询。
3. 适用范围：Keyed Stream与Non-key Stream，流中元素为*Point*。
4. 实现过程
   1. 数据准备。使用HashMap< GridID, List < Point >>存储窗口中的待查点。
   2. 格网搜索（筛选）。对查询点所在格网进行查询，若点的数量未超过k，则继续向外围扩张，直到获取了足以覆盖k个近邻点的格网集合。
   3. ...



### Range Query

1. 输入：描述查询范围的单个/多个**QueryGeometry**、轨迹点数据流**S**。

2. 作用：基于Filter算子。逐轨迹点滤得轨迹点数据流中落在一个/多个**QueryGeometry**中的轨迹点。

3. 适用范围：Keyed Stream与Non-key Stream，流中元素为*Point*。

4. 实现思路：

   1. UGridIndex实现(待完成)：首先取QueryGeometry的外包矩形覆盖点格网集合，通过空间关系计算得到格网集合中包含于查询范围中的格网S1，与查询范围存在交叉的格网集S2。将落在S1中的轨迹点输出，落在S2中的轨迹点需要进行进一步的计算。
   2. H3Index实现：首先取内含于QueryGeometry中的格网集S1，之后对S1进行扩张，得到与QueryGeometry相交的格网集S2。落在S1中的轨迹点将直接输出，落在S2中的轨迹点需要进行进一步的计算。

5. 算子声明（H3Index实现）：

   ```java
   DataStream<Point> result = RangeQuery.spatialRangeQuery(
     DataStream<Point> geoDataStream,
     U extends Geometry queryGeometry,
     int partitionNum, 
     int res);
   ```

   - geoDataStream：待查询点轨迹点流
   - queryGeometry：查询范围，继承自Geometry。
   - partitionNum：指定待查询轨迹点流的分区的数量。当指定为3时，Glink将根据轨迹流中车辆ID的哈希值为其分配0～2的键值。
   - res：为H3Index设置一个起始的格网分辨率。





## 空间连接(Spatial Join)
+ 单数据集连接
+ 多数据集连接

# 空间分析

## 空间聚类