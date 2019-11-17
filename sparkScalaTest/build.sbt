name := "sparkScalaTest"

version := "0.1"

scalaVersion := "2.10.5"

mainClass in (Compile, packageBin) := Some("BootstrappingEarthquakesFiji")
//mainClass in (Compile, packageBin) := Some("ProfessorSalary")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "au.com.bytecode" % "opencsv" % "2.4"
)