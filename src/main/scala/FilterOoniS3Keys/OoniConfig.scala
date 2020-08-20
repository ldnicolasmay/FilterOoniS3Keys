package FilterOoniS3Keys

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

object OoniConfig {

  // Config: Get OONI S3 bucket and prefix from ooni.conf
  val ooniConfig: Config = ConfigFactory.parseFile(new File("src/main/resources/config/ooni.conf"))
  val ooniBucketName: String = ooniConfig.getString("ooni.bucketName")
  val ooniPrefixRoot: String = ooniConfig.getString("ooni.prefixRoot")
  val ooniPrefixDates: List[String] = ooniConfig.getStringList("ooni.prefixPostfixes").toList
  val ooniTargetTestNames: List[String] = ooniConfig.getStringList("ooni.targetTestNames").toList

}
