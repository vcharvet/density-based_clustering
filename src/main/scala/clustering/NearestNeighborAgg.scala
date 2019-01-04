package clustering

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}


case class GroupRow(j: Long, distIJ: Double) //@deprecated
case class GroupBuffer(sortedList: Seq[(Long, Double)])

//@TODO add parameter, finish different for 'optics' or 'hdbscan"
class NearestNeighborAgg(k: Int, idCol: String, distCol: String) extends Aggregator[Row, GroupBuffer, (Long, Double)]
	with Serializable {
	override def bufferEncoder: Encoder[GroupBuffer] = Encoders.kryo[GroupBuffer]

		override def outputEncoder: Encoder[(Long, Double)] = Encoders.tuple(Encoders.scalaLong, Encoders.scalaDouble)
//	override def outputEncoder: Encoder[Map[Long, Double]] = Encoders.kryo[Map[Long, Double]]

	override def zero: GroupBuffer = GroupBuffer(Seq[(Long, Double)]())

	override def merge(b1: GroupBuffer, b2: GroupBuffer): GroupBuffer = {
//		val sortedTuples = (b1.sortedList ++ b2.sortedList).sortBy(_._2)
		val sortedTuples = mergeSeq[Long](b1.sortedList, b2.sortedList)

		GroupBuffer(sortedTuples)
	}

	override def reduce(buffer: GroupBuffer, input: Row): GroupBuffer = {
		val id = input.getAs[Long](idCol)
		val dist = input.getAs[Double](distCol)
		GroupBuffer(sortInsert(buffer.sortedList, (id, dist) ))
	}

	override def finish(buffer: GroupBuffer): (Long, Double) = {
		// retrieve kth-nearest neighbor and distance
		val couple = buffer.sortedList(k-1)
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

	def mergeSeq[U](seq1: Seq[(U, Double)], seq2: Seq[(U, Double)]): Seq[(U, Double)] = {
		(seq1, seq2) match {
			case (Nil, Nil) => Nil
			case (Nil, seq) => seq
			case (seq, Nil) => seq
			case (head1 +: tail1 , head2 +: tail2) => {
				if (head1._2 <= head2._2) Seq(head1, head2) ++ mergeSeq[U](tail1, tail2)
				else Seq(head2, head1) ++ mergeSeq[U](tail1, tail2)
				}
		}
	}

}
