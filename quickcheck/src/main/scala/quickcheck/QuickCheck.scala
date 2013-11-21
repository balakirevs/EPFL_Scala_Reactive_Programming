package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {
  def anyRepeats[T](l: List[T]): Boolean =
    if (l.isEmpty) false
    else if (l.length == 1) false
    else (for {
        a <- 0 until l.length
        b <- 0 until a
      } yield l(a) == l(b)).reduceLeft((a, b) => a || b)
  
  def genList(h: H): List[Int] = {
    def genListAcc(h: H, acc: List[Int]): List[Int] =
      if (isEmpty(h)) acc
      else            genListAcc(deleteMin(h), acc :+ findMin(h))
    genListAcc(h, List())
  }
  
  // Filtering out the first and second bogus
  property("bogusOneTwo") = forAll { (a: Int, h: H) =>
    if (a <= findMin(h)) findMin(insert(a, h)) == a
    else                 true
  }
  
  // Filtering out the third bogus
  property("bogusThreeFour") = forAll { (h: H, as: List[Int]) =>
    def containsAll[T](ts1: List[T], ts2: List[T]): Boolean =
      if (ts2.isEmpty) true
      else ts2.map(a => ts1.contains(a)).reduceLeft((a, b) => a && b)
    containsAll(genList(as.foldLeft(h)((b, a) => insert(a, b))), as)
  }
  
  // Filtering out the last bogus
  property("bogusFive") = forAll { (h1: H, h2: H) =>
    val l1 = genList(h1)
    val l2 = genList(h2)
    
    genList(meld(h1, h2)).length == (l1 ++ l2).length
  }

  // Generating a heap
  lazy val genHeap: Gen[H] = for {
    a <- arbitrary[Int]
    m <- oneOf(value(empty), genHeap)
  } yield insert(a, m)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)
}
