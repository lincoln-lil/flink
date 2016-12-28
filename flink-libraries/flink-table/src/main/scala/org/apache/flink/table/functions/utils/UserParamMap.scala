package org.apache.flink.table.functions.utils

import scala.collection.mutable

class UserParamMap(val map: mutable.Map[String,String]) extends Serializable{

  def this() = {
    this(new mutable.HashMap[String,String]())
  }

  def add(key: String, value: String): UserParamMap = {
    map.put(key, value)
    this
  }

  def get(key: String): String = map.get(key).asInstanceOf[String]
}

object UserParamMap{
  val Empty = new UserParamMap

  def apply(): UserParamMap = new UserParamMap
}