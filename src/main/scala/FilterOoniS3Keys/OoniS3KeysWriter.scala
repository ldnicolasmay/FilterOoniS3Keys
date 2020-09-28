package FilterOoniS3Keys

import java.io.File

import FilterOoniS3Keys.OoniConfig._
import FilterOoniS3Keys.PersistentS3Client._
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing}

import scala.collection.JavaConversions._
import scala.collection.parallel.ParMap


object OoniS3KeysWriter {

  /**
   * Gets next page of filtered S3 keys; if results are truncated this is called recursively
   *
   * @param listing    S3 objects listing
   * @param filterKeys Function to filter list of S3 keys
   * @param keys       Existing list of S3 keys
   * @return List of S3 keys
   */
  @scala.annotation.tailrec
  def getNextPageS3Keys(
                         listing: ObjectListing,
                         filterKeys: String => Boolean = _ => true,
                         keys: List[String] = Nil
                       ): List[String] = {
    val pageKeys = listing.getObjectSummaries.map(_.getKey).toList.filter(filterKeys)

    if (listing.isTruncated) {
      val nextListing: ObjectListing = s3Client.listNextBatchOfObjects(listing)
      getNextPageS3Keys(nextListing, filterKeys, pageKeys ::: keys)
    } else {
      pageKeys ::: keys
    }
  }

  /**
   * Simplifies retrieval of potentially paginated S3 keys and returns result of call to getNextPageS3Keys
   *
   * @param bucketName    S3 bucket name
   * @param keyPrefixRoot S3 keys prefix root
   * @param filterKeys    Function to filter list of S3 keys
   * @param keyPrefixStem S3 keys prefix stem
   * @return List of S3 keys
   */
  def getPaginatedS3Keys(
                          bucketName: String,
                          keyPrefixRoot: String,
                          filterKeys: String => Boolean
                        )(keyPrefixStem: String): List[String] = {
    val request: ListObjectsRequest = new ListObjectsRequest()
      .withBucketName(bucketName)
      .withPrefix(keyPrefixRoot + keyPrefixStem)
      .withMaxKeys(1000)
    val listing: ObjectListing = s3Client.listObjects(request)
    getNextPageS3Keys(listing, filterKeys)
  }

  /**
   * Writes list of S3 keys to local files after applying filterKeys function
   *
   * @param wd            Working directory
   * @param filteredKeys  List of filtered keys to write to file
   * @param keyPrefixStem S3 prefix stem, e.g., "2020-01"
   */
  def writeS3KeysToFile(
                         wd: os.Path,
                         filteredKeys: List[String]
                       )(keyPrefixStem: String): Unit = {
    // Write each S3 key to a line
    val fileName: String = s"ooni_s3_keys_$keyPrefixStem.dat"
    filteredKeys.foreach(key => os.write.append(wd / fileName, key + "\n"))
  }

  /**
   * Writes local file list of S3 keys to S3 bucket after applying filterKeys function
   *
   * @param wd         Working directory
   * @param bucketName Target S3 bucket name
   * @param keyPrefix  Target key prefix
   * @param keySuffix  Source and target S3 key suffix, e.g., "2020-01"
   */
  def writeS3KeysToTargetS3(
                             wd: os.Path,
                             bucketName: String,
                             keyPrefix: String
                           )(keySuffix: String): Unit = {
    // TODO: Revise to accept filtered keys (List[String]),
    //  write filtered keys to temp file, then write temp file to S3
    val wdStr: String = wd.toString()
    val fileName: String = s"ooni_s3_keys_$keySuffix.dat"
    val file: File = new File(wdStr + "/" + fileName)
    val targetKey: String = s"$keyPrefix/ooni_s3_keys_$keySuffix.dat"

    try
      s3Client.putObject(bucketName, targetKey, file)
    catch {
      case e: AmazonServiceException => println(e)
    }
  }

  /**
   * Driver for OoniS3KeysWriter Spark app
   *
   * @param args Arguments passed to main method
   */
  def main(args: Array[String]): Unit = {

    // Create filter predicate from OONI target test names defined in ooni.conf
    val keyFilterRegex: String = ".*(" + ooniTargetTestNames.mkString("|") + ").*"
    val filterKeys = (x: String) => x.matches(keyFilterRegex)

    // Retrieve filtered keys, mapped to dates
    val filteredKeysMap: ParMap[String, List[String]] = ooniPrefixDates.par.map { date =>
      val keys = getPaginatedS3Keys(ooniBucketName, ooniPrefixRoot, filterKeys)(date)
      date -> keys
    }.toMap

    // Write text files in parallel to disc
    val wd: os.Path = os.pwd / os.RelPath("src/main/resources/data")
    os.makeDir.all(wd)
    filteredKeysMap.par.foreach {
      case (date, filteredKeys) => writeS3KeysToFile(wd, filteredKeys)(date)
    }

    // Write text files in parallel to target S3 bucket
    val targetBucketName: String = "udacity-ooni-project"
    val targetKeyPrefix: String = "keys"
    filteredKeysMap.par.foreach {
      case (date, _) => writeS3KeysToTargetS3(wd, targetBucketName, targetKeyPrefix)(date)
    }

  }
}
