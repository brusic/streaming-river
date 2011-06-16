import sbt._

class StreamingRiverProject(info: ProjectInfo) extends DefaultProject(info) {

  val sonatypeReleases = "Sonatype Maven2 Release Repository" at "http://oss.sonatype.org/content/repositories/releases/"
  val sonatypeSnapshots = "Sonatype Maven2 Snapshots Repository" at "http://oss.sonatype.org/content/repositories/snapshots/"

  val pluginName = "streaming-river"
  val dispatchVersion = "0.8.3"
  val elasticsearchVersion = "0.16.2"
  val elasticsearchJarPluginName = "elasticsearch-%s-%s.zip".format(pluginName, elasticsearchVersion)

  val elasticsearch = "org.elasticsearch" % "elasticsearch" % elasticsearchVersion
  val dispatch_http = "net.databinder" %% "dispatch-http" % dispatchVersion
  val dispatch_nio = "net.databinder" %% "dispatch-nio" % dispatchVersion

  def runJars = {
    (managedDependencyPath / Configurations.Compile.toString ##) * ("*.jar" - "lucene-*.jar" - "elasticsearch*jar") +++
    ((outputPath ##) / defaultJarName) +++
    mainDependencies.scalaJars
  }

  lazy val `package-elasticsearch` = packageElasticsearch
  lazy val packageElasticsearch =
    zipTask(runJars, outputPath / pluginName, elasticsearchJarPluginName)
      .dependsOn(`package`)
      .describedAs("Zips up the simplewebsocket.plugin using the required elasticsearch filename format.")

  lazy val specsDependency =
    (if (buildScalaVersion startsWith "2.7.")
      "org.scala-tools.testing" % "specs" % "1.6.2.2"
    else
      "org.scala-tools.testing" %% "specs" % "1.6.8") % "test"

}
