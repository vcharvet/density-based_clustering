package clustering

import org.apache.spark.sql.DataFrame

/* the objective of class SpanningTree is to build the minimum spanning tree of the mutual
reachability graph. The mutual reachability graph is also computed in this class

 */
class SpanningTree {
	/** computed mutual reachability graph based on core distance info in df

		@param df : DataFrame
		          	cols are [vecotriID, Map(vectorjID, dist(i, j))]
								or [vectoriID, vectorjID, dist(i, j)] or [vectori, (vectorjID, dist(i,j))]
	 **/
	def mutualReachabilityGraph(df: DataFrame, iCol: String, distIJ: String): Unit = {

	}


}
