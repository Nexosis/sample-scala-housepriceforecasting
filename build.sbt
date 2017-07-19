name := "sample-scala-housingprices"

version := "1.0"

scalaVersion := "2.12.2"

resolvers +=
  Resolver.sonatypeRepo("public")


libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/com.nexosis/nexosisclient-java
  "com.nexosis" % "nexosisclient-java" % "1.1.0",
  // https://mvnrepository.com/artifact/org.jfree/jfreechart
  "org.jfree" % "jfreechart" % "1.0.19"
)
