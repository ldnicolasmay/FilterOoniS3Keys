package OoniKeys

import java.io.File

import OoniKeys.OoniConfig._
import OoniKeys.PersistentS3Client._
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing}

import scala.collection.JavaConversions._


object OoniS3KeysWriter {

  def main(args: Array[String]): Unit = {

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
     * Simplifies retrieval of potentially paginated S3 keys
     * Returns result of call to getNextPageS3Keys
     *
     * @param bucketName S3 bucket name
     * @param prefix     S3 keys prefix
     * @param filterKeys Function to filter list of S3 keys
     * @return List of S3 keys
     */
    def getPaginatedS3Keys(
                            bucketName: String,
                            prefix: String,
                            filterKeys: String => Boolean
                          ): List[String] = {
      val request: ListObjectsRequest = new ListObjectsRequest()
        .withBucketName(bucketName)
        .withPrefix(prefix)
        .withMaxKeys(1000)
      val listing: ObjectListing = s3Client.listObjects(request)
      getNextPageS3Keys(listing, filterKeys)
    }

    /**
     * Writes list of S3 keys to files after applying filterKeys function
     *
     * @param wd                Working directory
     * @param ooniBucketName    OONI S3 bucket name
     * @param ooniPrefixRoot    OONI S3 prefix root
     * @param filterKeys        Function to filter list of S3 keys
     * @param ooniPrefixPostfix OONI S3 prefix postfix
     */
    def writeS3KeysToFile(
                           wd: os.Path,
                           ooniBucketName: String,
                           ooniPrefixRoot: String,
                           filterKeys: String => Boolean
                         )(ooniPrefixPostfix: String): Unit = {
      val ooniPrefix: String = ooniPrefixRoot + ooniPrefixPostfix
      val s3KeysFiltered: List[String] = getPaginatedS3Keys(ooniBucketName, ooniPrefix, filterKeys)

      // Write each S3 key to a line
      val fileName: String = s"ooni_s3_keys_$ooniPrefixPostfix.dat"
      s3KeysFiltered.foreach(s3key => os.write.append(wd / fileName, s3key + "\n"))
      //println(s"OONI S3 keys written to $wd/$fileName")
    }

    def writeS3KeysToTargetS3(
                               wd: os.Path,
                               targetBucketName: String
                             )(sourcePrefixPostfix: String): Unit = {
      val wdStr: String = wd.toString()
      val fileName: String = s"ooni_s3_keys_$sourcePrefixPostfix.dat"
      val file: File = new File(wdStr + "/" + fileName)
      val targetPrefix: String = s"ooni_s3_keys_$sourcePrefixPostfix.dat"
      try
        s3Client.putObject(targetBucketName, targetPrefix, file)
      catch {
        case e: AmazonServiceException => println(e)
      }
    }

    val keyFilterRegex: String = ".*(" + ooniTargetTestNames.mkString("|") + ").*"
    val filterKeys = (x: String) => x.matches(keyFilterRegex)

    // Write text files in parallel to disc
    val wd: os.Path = os.pwd / os.RelPath("src/main/resources/data")
    os.makeDir.all(wd)
    ooniPrefixDates.par.foreach { date =>
      writeS3KeysToFile(wd, ooniBucketName, ooniPrefixRoot, filterKeys)(date)
    }

    // Write text files in parallel to target S3 bucket
    val targetBucketName: String = "udacity-ooni-project"
    ooniPrefixDates.par.foreach { date =>
      writeS3KeysToTargetS3(wd, targetBucketName)(date)
    }

  }
}
