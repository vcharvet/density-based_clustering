package org.local.clustering


import collection.mutable.HashMap
import scala.collection.mutable
/** implements a disjoint set data structure for Kruskal algorithm
	*
	*  @TODO add method to merge to DisjointSets
	* taken from http://www.bistaumanga.com.np/blog/unionfind/
	*/
class DisjointSet[T] extends Serializable {
	private val parent = new mutable.HashMap[T, T]
	private val rank = new mutable.HashMap[T, Int]

	def getParent(): Seq[(T, T)] = parent.toSeq

	def size = parent.size

	def +=(x: T) = add(x)
	def ++(x: T) = add(x)

	def add(x: T): DisjointSet[T] ={
		parent += (x -> x)
		rank += (x -> 0)
		this
	}

	def union(x: T, y: T): DisjointSet[T] ={
		val s = find(x)
		val t = find(y)
		if(rank(s) > rank(t)) parent += (t -> s)
		else{
			if(rank(s) == rank(t)) rank(t) += 1
			parent += (s -> t)
		}
		this
	}

	def find(x: T): T = {
		if(parent(x) == x) x
		else {
			parent += (x -> find(parent(x)))
			parent(x)
		}
	}

	def merge(other: DisjointSet[T]) = {
		parent ++ other.parent
		rank ++ other.rank
		this
	}

	def areConnected(x: T, y: T): Boolean = find(x) == find(y)

	override def toString: String = parent.toString
}
