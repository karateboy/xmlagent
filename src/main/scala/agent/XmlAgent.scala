package agent

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.scalalogging._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

object XmlAgent extends LazyLogging {

  import com.typesafe.config.ConfigFactory

  case object ParseOutput

  private val config = ConfigFactory.load()

  private val inputPath = config.getString("inputPath")
  logger.info(s"inputPath =$inputPath")

  private val xmlOutputPathStr = config.getString("xmlOutputPath")
  logger.info(s"xmlOutputPathStr =$xmlOutputPathStr")

  private val itemType = config.getString("itemType")
  logger.info(s"itemType =$itemType")

  private val fileNameConfig = config.getInt("fileNameConfig")
  logger.info(s"fileNameConfig =$fileNameConfig")

  private def configType = List("ic01", "ts01", "voc01")

  private def getChannelMap(name: String) = {
    val channels = config.getObject(name).entrySet()
    val channelKV = channels.asScala map { ch =>
      val v = ch.getValue.render()
      (ch.getKey, v.substring(1, v.length() - 1))
    }
    channelKV.toMap
  }

  private def getAnMap(name: String) = {
    val ans = config.getObject(name).entrySet()
    val anKV = ans.asScala map { an =>
      val v = an.getValue.render()
      (an.getKey, v.substring(1, v.length() - 1))
    }
    anKV.toMap
  }

  private val configTypeMap = configType.map {
    name =>
      name -> (
        config.getObject(s"${name}_prop"),
        getChannelMap(s"${name}_channel"),
        getAnMap(s"${name}_an"))
  }.toMap

  private def getChannelFolderMap: Map[String, Map[String, String]] = configType.map {
    name =>
      name->getAnMap(s"${name}_folder")
  }.toMap

  var receiver: ActorRef = _

  def startup(system: ActorSystem): Unit = {
    receiver = system.actorOf(Props(classOf[XmlAgent]), name = "xmlAgent")
  }

  def parseOutput(): Unit = {
    receiver ! ParseOutput
  }
}

class XmlAgent extends Actor with LazyLogging {

  import XmlAgent._
  import com.github.nscala_time.time.Imports._
  import org.apache.commons.io._

  import java.io.File
  import java.nio.file._

  val localDir = new File("local" + File.separator)
  if (!localDir.exists())
    localDir.mkdirs()
  else
    FileUtils.cleanDirectory(localDir)

  logger.info(s"localPath=${localDir.getAbsolutePath}")

  val xmlOutDir = new File(XmlAgent.xmlOutputPathStr)
  if (!xmlOutDir.exists()) {
    val xmlOutPath = Paths.get(xmlOutDir.getAbsolutePath)
    Files.createDirectories(xmlOutPath)
  }

  logger.info(s"xmlOutPath=${xmlOutDir.getAbsolutePath}")

  private def writeXml(dt: DateTime, computer: String, channel: String, mtDataList: List[(String, String)]) = {
    try {

      val (props, channelMap, anMap) = configTypeMap(computer.toLowerCase())

      val dtStr = dt.toString("YYYYMMddHHmmss")
      val glass_id = props.toConfig.getString("glass_id")
      val eqid = props.toConfig.getString("eqp_id")
      val xmlFile =
        if (fileNameConfig == 0)
          new File(s"$xmlOutputPathStr${File.separator}${dtStr}_${glass_id}_$eqid.xml")
        else if (fileNameConfig == 1) {
          val outpathStr = s"$xmlOutputPathStr${File.separator}$eqid${File.separator}${channelMap(channel)}${File.separator}"
          val outpath = new File(outpathStr)
          if (!outpath.exists())
            outpath.mkdirs()

          new File(s"$outpathStr${dtStr}_${channelMap(channel)}_${glass_id}.xml")
        } else if (fileNameConfig == 2) {
          val outpathStr = s"$xmlOutputPathStr${File.separator}$eqid${File.separator}"
          val outpath = new File(outpathStr)
          if (!outpath.exists())
            outpath.mkdirs()

          new File(s"$outpathStr${dtStr}_${channelMap(channel)}_$glass_id.xml")
        } else if(fileNameConfig == 3){
          val channelFolderMap = getChannelFolderMap
          val outpathStr = s"$xmlOutputPathStr${File.separator}$eqid${File.separator}${channelFolderMap(computer.toLowerCase())(channel)}${File.separator}"
          val outpath = new File(outpathStr)
          if (!outpath.exists())
            outpath.mkdirs()

          new File(s"$outpathStr${dtStr}_${channelMap(channel)}_${glass_id}.xml")
        }else{
          throw new Exception(s"Unknwon fileNameConfig ${fileNameConfig}")
        }

      val nodeBuffer = new xml.NodeBuffer
      val keyConfigList = props.asScala.toList

      for {
        (key, v) <- keyConfigList.sortWith((cv1, cv2) => cv1._2.origin().lineNumber() < cv2._2.origin().lineNumber())
      } {
        key match {
          case "cldate" => nodeBuffer += <cldate>
            {dt.toString("YYYY-MM-dd")}
          </cldate>
          case "cltime" => nodeBuffer += <cltime>
            {dt.toString("HH:mm:ss")}
          </cltime>
          case "sub_eqp_id" if fileNameConfig == 1 =>
            nodeBuffer += xml.Elem(null, key, xml.Null, xml.TopScope, false, new xml.Text(channelMap(channel)))
          case "eqp_id" if fileNameConfig == 1 =>
            nodeBuffer += xml.Elem(null, key, xml.Null, xml.TopScope, false, new xml.Text(channelMap(channel)))
          case _ =>
            val str = v.unwrapped().asInstanceOf[String]
            nodeBuffer += xml.Elem(null, key, xml.Null, xml.TopScope, false, new xml.Text(str))
        }
      }

      val iaryBuffer = new xml.NodeBuffer()
      for (mtData <- mtDataList) {
        val iary = <iary>
          <item_name>
            {s"${channelMap(channel)}${anMap(mtData._1)}"}
          </item_name>
          <item_type>
            {s"$itemType"}
          </item_type>
          <item_value>
            {mtData._2}
          </item_value>
        </iary>
        iaryBuffer += iary
      }
      val dataElem = <datas>
        {iaryBuffer}
      </datas>
      nodeBuffer += dataElem
      val outputXml = <EDC>
        {nodeBuffer}
      </EDC>
      val outputFilename = xmlFile.getAbsolutePath

      xml.XML.save(
        filename = outputFilename,
        enc = "Big5",
        node = outputXml,
        xmlDecl = true)
      logger.info(s"write $outputFilename done")

      //copyXml(eqid, glass_id)
      true
    } catch {
      case ex: Exception =>
        logger.error("failed to output", ex)
        false
    }
  }

  def receive: Receive = {
    case ParseOutput =>
      try {
        processInputPath(parser)
      } catch {
        case ex: Throwable =>
          logger.error("processInputPath failed", ex)
      }
      context.system.scheduler.scheduleOnce(scala.concurrent.duration.Duration(1, scala.concurrent.duration.MINUTES), self, ParseOutput)
  }

  private def parser(f: File): Boolean = {
    import java.nio.charset.StandardCharsets
    import java.nio.file.{Files, Paths}
    import scala.collection.JavaConverters._

    val lines =
      try {
        Files.readAllLines(Paths.get(f.getAbsolutePath), StandardCharsets.ISO_8859_1).asScala
      } catch {
        case ex: Throwable =>
          logger.error("rvAgent", "failed to read all lines", ex)
          Seq.empty[String]
      }

    if (lines.isEmpty) {
      false
    } else {
      def recordParser(unparsed: scala.collection.Seq[String]): List[(DateTime, String, String, List[(String, String)])] = {
        if (unparsed.length < 2)
          Nil
        else {
          var lineNo = 0
          var dt: Option[DateTime] = None
          var data = List.empty[(String, String)]
          var computer = ""
          var channel = ""
          try {
            dt = Some(DateTime.parse(unparsed(lineNo), DateTimeFormat.forPattern("YYYY/MM/dd HH:mm")))
            lineNo += 1
            while (lineNo < unparsed.length) {
              val unparsed_line = unparsed(lineNo)
              val elements = unparsed_line.split(";").toList
              computer = elements(1)
              channel = elements(2)
              val mt = elements(3)
              val v = elements(4)
              logger.info(s"ch=$channel mt=$mt value=$v")
              data = data :+ (mt, v)
              lineNo += 1
            }
            (dt.get, computer, channel, data) :: Nil
          } catch {
            case ex: java.lang.IndexOutOfBoundsException =>
              if (dt.isDefined && data.nonEmpty)
                (dt.get, computer, channel, data) :: recordParser(unparsed.drop(lineNo))
              else
                recordParser(unparsed.drop(lineNo))

            case ex: Throwable =>
              logger.error("unexpected error", ex)
              recordParser(Seq.empty[String])
          }
        }
      }

      val records = recordParser(lines)
      logger.info(s"record = ${records.length}")
      val ret =
        for (rec <- records) yield {
          writeXml(rec._1, rec._2, rec._3, rec._4)
        }

      ret.forall(y => y)
    }
  }

  private def listAllFiles(files_path: String) = {
    //import java.io.FileFilter
    val path = new java.io.File(files_path)
    if (path.exists() && path.isDirectory) {
      val allFiles = new java.io.File(files_path).listFiles().toList
      allFiles.filter(p => p != null)
    } else {
      logger.warn(s"invalid input path ${files_path}")
      List.empty[File]
    }
  }

  def copyXml(eqid: String, glass_id: String) = {
    val files = listAllFiles(localDir.getAbsolutePath)
    import java.nio.file._

    val xmlPath = Paths.get(xmlOutDir.getAbsolutePath)
    val xmlFiles = files.filter(_.getName.endsWith("xml"))

    try {
      import org.apache.commons.io.filefilter.TrueFileFilter
      val targets = FileUtils.listFilesAndDirs(localDir, TrueFileFilter.TRUE, null).asScala
      targets.foreach {
        f =>
          logger.info(s"list => ${f.getAbsolutePath}")
          if (f.isDirectory)
            FileUtils.copyDirectoryToDirectory(f, xmlOutDir)
          else
            FileUtils.copyFileToDirectory(f, xmlOutDir)
      }

      FileUtils.cleanDirectory(localDir)
      /*
      xmlFiles.map {
        f =>
          val localPath = Paths.get(f.getAbsolutePath)
          import org.apache.commons.io._

          //FileUtils.copyFileToDirectory(f, xmlOutDir, true)
          f.delete()
      }*/
      true
    } catch {
      case ex: Throwable =>
        logger.error(ex.toString)
        false
    }
  }

  private def processInputPath(parser: File => Boolean): Unit = {
    val files = listAllFiles(XmlAgent.inputPath)

    for (f <- files) {
      if (f.getName.endsWith("txt")) {
        logger.info(s"parse ${f.getName}")
        try {
          val result = parser(f)

          if (result) {
            logger.info(s"${f.getAbsolutePath} success.")
            f.delete()
          }
        } catch {
          case ex: Throwable =>
            logger.error("skip buggy file", ex)
        }
      } else {
        f.delete()
      }
    }
  }

  override def postStop: Unit = {

  }
}