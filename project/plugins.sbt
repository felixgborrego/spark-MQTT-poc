addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.9")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.3")

// protobuf code generation
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.0")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.10.10"

addDependencyTreePlugin
