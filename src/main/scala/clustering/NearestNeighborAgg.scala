package clustering

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row, TypedColumn}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/** user defined aggregation function to fetch nearest neighbors of a collection of
	* points in a dataframe
	*
	* @deprecated use  other class instead
	* It is suposed to be used for a dataframe (i, j, distance(i,j)) grouped by key i
	* NearestNeighborAgg fetches the kth nearest neighbor in each group and its distance
	*
	*/
class NearestNeighborAgg(k: Int, idCol: String, distanceCol: String) extends UserDefinedAggregateFunction {
	// the input rows are (j, dist(i, j))
	def inputSchema: StructType = StructType(
		StructField(idCol, LongType) :: StructField(distanceCol, DoubleType) :: Nil)

		//TODO find better implementation, with Params ??


	/* override bufferSchema
		sortedDistance contains the sequence of distances to i
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

	//TODO how to unit test private function?
	def sortInsert(currentDistances: Seq[Double], newDist: Double): Seq[Double] = {
	currentDistances match {
		case head +: tail => if (newDist < head)  Seq[Double](newDist +: head +:  tail: _*)
			else head +: sortInsert(tail, newDist)
		case head +: Nil =>  Seq[Double](head, newDist)
		case Nil => Seq(newDist)
		}
	}
}

case class GroupRow(j: Long, distIJ: Double) //extends GenericRowWithSchema
case class GroupBuffer(sortedList: Seq[(Long, Double)])

//@TODO add parameter, finish different for 'optics' or 'hdbscan"
class NearestNeighborAgg2(k: Int, colName: String) extends Aggregator[Row, GroupBuffer, (Long, Double)]
	with Serializable {
	override def bufferEncoder: Encoder[GroupBuffer] = Encoders.kryo[GroupBuffer]

		override def outputEncoder: Encoder[(Long, Double)] = Encoders.tuple(Encoders.scalaLong, Encoders.scalaDouble)
//	override def outputEncoder: Encoder[Map[Long, Double]] = Encoders.kryo[Map[Long, Double]]

	override def zero: GroupBuffer = GroupBuffer(Seq[(Long, Double)]())

	override def merge(b1: GroupBuffer, b2: GroupBuffer): GroupBuffer = {
	//TODO optimize merge
		var sortedTuples = Seq[(Long, Double)]()

		for (x <- b1.sortedList){sortedTuples = sortInsert(sortedTuples, x)}
		for (x <- b2.sortedList){sortedTuples = sortInsert(sortedTuples, x)}

		GroupBuffer(sortedTuples)
	}

	override def reduce(buffer: GroupBuffer, input: Row): GroupBuffer = {
		val castedRow = input.getAs[Row](colName)
		GroupBuffer(sortInsert(buffer.sortedList,
			(castedRow.getAs[Long](0), castedRow.getAs[Double](1))))
	}

	override def finish(buffer: GroupBuffer): (Long, Double) = {
//		GroupRow(buffer.sortedList(k-1)._1, buffer.sortedList(k-1)._2)
		// retrieve kth-nearest neighbor and distance
		val couple = buffer.sortedList(k-1)
//		Map[Long, Double]((couple._1, couple._2))
		couple
	}


	def sortInsert(currentList: Seq[(Long, Double)], newTuple: (Long, Double)): Seq[(Long, Double)] = {
		currentList match {
			case head +: tail =>
				if (newTuple._2 > head._2) head +: sortInsert(tail, newTuple)
				else newTuple +: head +: tail
			case head +: Nil => Seq(head, newTuple)
			case Nil => Seq(newTuple)
		}
	}

}
