package hive.scala.udtf

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.{PrimitiveObjectInspector,ObjectInspector,StructObjectInspector,ObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

class ExpandTree2UDTF extends GenericUDTF {
  var inputOIs: Array[PrimitiveObjectInspector] = null
  val tree: collection.mutable.Map[String,Option[String]] = collection.mutable.Map()

  case class MyValueType(acct: String, grpName: String, acctType: String)

  //The Hive calls the initialize method to notify the UDTF the argument types to expect.
  //The UDTF must then return an object inspector corresponding to the row objects that the UDTF will generate.
  override def initialize(args: Array[ObjectInspector]): StructObjectInspector = {
    inputOIs = args.map{_.asInstanceOf[PrimitiveObjectInspector]}
    val fieldNames = java.util.Arrays.asList("id", "ancestor", "level")
    val fieldOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector.asInstanceOf[ObjectInspector]
    val fieldOIs = java.util.Arrays.asList(fieldOI, fieldOI, fieldOI)
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  // Give a set of arguments for the UDTF to process.
  //the below method gets called for every row
  //UDTF can produce and forward rows to other operators by calling forward()
  def process(record: Array[Object]) {
    val id = inputOIs(0).getPrimitiveJavaObject(record(0)).asInstanceOf[String]

    val parent: Option[String] = inputOIs(1).getPrimitiveJavaObject(record(1)).asInstanceOf[String] match {
      case r if r.isEmpty => None
      case r              => Some(r)
    }

    // "parent" is defined as Optional type, "Some" or "None"
    // if parent is empty, that's should be "None"
    tree += ( id -> parent )
  }

  // Called to notify the UDTF that there are no more rows to process.
  // Clean up code or additional forward() calls can be made here.
  def close {
    val expandTree = collection.mutable.Map[String,List[String]]()
    def calculateAncestors(id: String): List[String] =
      tree(id) match {
        case Some(parent) => id :: getAncestors(parent)
        case None => List(id)
      }
    def getAncestors(id: String) = expandTree.getOrElseUpdate(id, calculateAncestors(id))

    tree.keys.foreach {
      id => getAncestors(id).zipWithIndex.foreach {
        case(ancestor,level) => forward(Array(id, ancestor, level))
      }
    }
  }
}
