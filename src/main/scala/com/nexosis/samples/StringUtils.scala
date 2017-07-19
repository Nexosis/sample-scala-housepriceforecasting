package com.nexosis.samples

object StringUtils {
  implicit class StringImprovements(val s: String) {
    import scala.util.control.Exception._
    //def toIntOpt = catching(classOf[NumberFormatException]) opt s.toInt
    def toDoubleOpt = catching(classOf[NumberFormatException]) opt s.toDouble
  }
}
