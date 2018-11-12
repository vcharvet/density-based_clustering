package clustering

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/** user defined aggregation function to fetch nearest neighbors of a collection of
	* points in a dataframe
	*
	* It is suposed to be used for a dataframe (i, j, distance(i,j)) grouped by key i
	* NearestNeighborAgg fetches the kth nearest neighbor in each group and its distance
	*
	*/
class NearestNeighborAgg(k: Int, idCol: String, distanceCol: String) extends UserDefinedAggregateFunction {
	// the input rows are (j, dist(i, j))
	def inputSchema: StructType = StructType(
		StructField(idCol, LongType) :: StructField(distanceCol, DoubleType) :: Nil)

		//TODO find better implementation, with Params ??
//		var k: Int = 2
//		def setK(k: Int): Unit = this.k = k


	/* override bufferSchema
		sortedDistance conatains the sequence of distances to i
		keyVales map(vector2Id to distance(vector1, vector2)
	 */
	def bufferSchema: StructType = {
		StructType(StructField("sortedDistances", ArrayType(DoubleType))
		:: StructField("keyValues", MapType(DoubleType, LongType)) :: Nil)}

	//type of the returned value
	def dataType: DataType = MapType(LongType, DoubleType) // TODO change

	override def deterministic: Boolean = true

	def initialize(buffer: MutableAggregationBuffer): Unit = {
		buffer(0) = Seq[Double]()
		buffer(1) = Map[Double, Long]()
		}

	def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
		if (!input.isNullAt(0)) {

			val newDist: Double = input.getAs[Double](1)
			val newId: Long = input.getAs[Long](0)

			val updatedDistances = this.sortInsert(buffer.getAs[Seq[Double]](0), newDist)

			buffer(0) = updatedDistances
			buffer(1) = buffer.getAs[Map[Double, Long]](1) + ((newDist, newId))
		}
	}

	def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
		// update key values
		buffer1(1) = buffer1.getAs[Map[Double, Long]](1) ++ buffer2.getAs[Map[Double, Long]](1)

		// update distances
		var distances = buffer2.getAs[Seq[Double]](0)
		for (newDistance1 <- buffer1.getSeq(0).iterator){
			distances = sortInsert(distances, newDistance1)
		}
		buffer1(0) = distances
	}

	def evaluate(buffer: Row): Map[Long, Double] = { //change return type to Map(Long, Double)
		val kDist = buffer.getAs[Seq[Double]](0).apply(k - 1)
		val kNeighbor = buffer.getAs[Map[Double, Long]](1).apply(kDist)

		Map[Long, Double]((kNeighbor, kDist))
	}

	//TODO how to unit test pricate function?
	def sortInsert(currentDistances: Seq[Double], newDist: Double): Seq[Double] = {
	currentDistances match {
		case head +: tail => if (newDist < head)  Seq[Double](newDist +: head +:  tail: _*)
			else head +: sortInsert(tail, newDist)
		case head +: Nil =>  Seq[Double](head, newDist)
		case Nil => Seq(newDist)
		}
	}

}
