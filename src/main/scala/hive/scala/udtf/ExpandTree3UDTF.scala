package hive.scala.udtf

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector, StructObjectInspector}

class ExpandTree3UDTF extends GenericUDTF {
  var inputOIs: Array[PrimitiveObjectInspector] = null
  val tree: collection.mutable.Map[String, MyValueType] = collection.mutable.Map()

  case class MyValueType(acct: String, grpName: String, acctType: String)

  //The Hive calls the initialize method to notify the UDTF the argument types to expect.
  //The UDTF must then return an object inspector corresponding to the row objects that the UDTF will generate.
  override def initialize(args: Array[ObjectInspector]): StructObjectInspector = {
    inputOIs = args.map {
      _.asInstanceOf[PrimitiveObjectInspector]
    }
    val fieldNames = java.util.Arrays.asList("acct", "grp", "acct_type", "parent_grp_1", "parent_grp_2", "parent_grp_3", "parent_grp_4")
    val fieldOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector.asInstanceOf[ObjectInspector]
    val fieldOIs = java.util.Arrays.asList(fieldOI, fieldOI, fieldOI, fieldOI, fieldOI, fieldOI,fieldOI)
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  // Give a set of arguments for the UDTF to process.
  //the below method gets called for every row
  //UDTF can produce and forward rows to other operators by calling forward()
  def process(record: Array[Object]) {
    val id = inputOIs(0).getPrimitiveJavaObject(record(0)).asInstanceOf[String]

    val parent: String = inputOIs(1).getPrimitiveJavaObject(record(1)).asInstanceOf[String] match {
      case r if r.isEmpty => ""
      case r => r
    }
    val grpType: String = inputOIs(2).getPrimitiveJavaObject(record(2)).asInstanceOf[String] match {
      case r if r.isEmpty => ""
      case r => r
    }

    // "parent" is defined as Optional type, "Some" or "None"
    // if parent is empty, that's should be "None"
    tree += (id -> MyValueType(id, parent, grpType))
  }

  // Called to notify the UDTF that there are no more rows to process.
  // Clean up code or additional forward() calls can be made here.

  def close {
    val expandTree = collection.mutable.Map[String, List[String]]()


    def calculateAncestors(grpName: String): List[String] = {
      if (tree.contains(grpName)) {
        tree(grpName) match {
          case s: MyValueType => s.acct :: getAncestorGroups(s.grpName)
          case _ => List(grpName)
        }
      }else{
        List(grpName)
      }
    }


    def getAncestorGroups(grpName: String) = expandTree.getOrElseUpdate(grpName, calculateAncestors(grpName))


    tree.values.filter {
      case s: MyValueType => if (s.acctType.equals("user")) true else false
      case _ => false

    }.foreach {
      _ match {
        case s: MyValueType => forward(Array(s.acct, s.grpName, s.acctType) ++ getAncestorGroups(s.grpName))
        case _ => None
      }

    }
  }
}
