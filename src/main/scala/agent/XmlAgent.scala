package agent

import akka.actor.{ Actor, ActorLogging, Props, ActorRef }
import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.scalalogging._
import scala.collection.JavaConverters._

object XmlAgent extends LazyLogging {
  import com.typesafe.config.ConfigFactory
  import com.github.nscala_time.time.Imports._
  import java.nio.file._

  case object ParseOutput

  val config = ConfigFactory.load()

  val inputPath = config.getString("inputPath")
  logger.info(s"inputPath =$inputPath")

  val xmlOutputPathStr = config.getString("xmlOutputPath")
  logger.info(s"xmlOutputPathStr =$xmlOutputPathStr")

  val itemType = config.getString("itemType")
  logger.info(s"itemType =$itemType")

  val fileNameConfig = config.getInt("fileNameConfig")
  logger.info(s"fileNameConfig =$fileNameConfig")

  def configType = List("ic01", "ts01", "voc01")

  def getChannelMap(name: String) = {
    val channels = config.getObject(name).entrySet()
    val channelKV = channels.asScala map { ch =>
      val v = ch.getValue.render()
      (ch.getKey, v.substring(1, v.length() - 1))
    }
    channelKV.toMap
  }

  def getAnMap(name: String) = {
    val ans = config.getObject(name).entrySet()
    val anKV = ans.asScala map { an =>
      val v = an.getValue.render()
      (an.getKey, v.substring(1, v.length() - 1))
    }
    anKV.toMap
  }

  val configTypeMap = configType.map {
    name =>
      name -> (
        config.getObject(s"${name}_prop"),
        getChannelMap(s"${name}_channel"),
        getAnMap(s"${name}_an"))
  }.toMap

  var receiver: ActorRef = _
  def startup(system: ActorSystem) = {
    receiver = system.actorOf(Props(classOf[XmlAgent]), name = "xmlAgent")
  }

  def parseOutput = {
    receiver ! ParseOutput
  }
}

import scala.concurrent.{ Future, Promise }

class XmlAgent extends Actor with LazyLogging {
  import XmlAgent._
  import com.typesafe.config.ConfigFactory
  import com.github.nscala_time.time.Imports._
  import java.io.File
  import java.nio.file._
  val localDir = new File("local" + File.separator)
  if (!localDir.exists())
    localDir.mkdirs()

  logger.info(s"localPath=${localDir.getAbsolutePath}")

  val xmlOutDir = new File(XmlAgent.xmlOutputPathStr)
  if (!xmlOutDir.exists()) {
    val xmlOutPath = Paths.get(xmlOutDir.getAbsolutePath)
    Files.createDirectories(xmlOutPath)
  }

  logger.info(s"xmlOutPath=${xmlOutDir.getAbsolutePath}")

  def writeXml(dt: DateTime, computer: String, channel: String, mtDataList: List[(String, String)]) = {
    try {

      val (props, channelMap, anMap) = configTypeMap(computer.toLowerCase())

      val dtStr = dt.toString("YYYYMMddHHmmss")
      val eqid = props.toConfig().getString("glass_id")
      val glass_id = props.toConfig().getString("eqp_id")
      val xmlFile =
        if (fileNameConfig == 0)
          new File(s"${localDir.getAbsolutePath}${File.separator}${dtStr}_${glass_id}_${eqid}.xml")
        else
          new File(s"${localDir.getAbsolutePath}${File.separator}${dtStr}_${channel}_${glass_id}_${eqid}.xml")

      val nodeBuffer = new xml.NodeBuffer
      for (p <- props.entrySet().asScala) {
        val v = p.getValue.render()
        nodeBuffer += xml.Elem(null, p.getKey, xml.Null, xml.TopScope, false, new xml.Text(v.substring(1, v.length() - 1)))
      }
      nodeBuffer += <cldate>{ dt.toString("YYYY-MM-dd") }</cldate>
      nodeBuffer += <cltime>{ dt.toString("HH:mm:ss") }</cltime>
      val iaryBuffer = new xml.NodeBuffer()
      for (mtData <- mtDataList) {
        val iary = <iary>
                     <item_name>{ s"${channelMap(channel)} ${anMap(mtData._1)}" }</item_name>
                     <item_type>{ s"$itemType" }</item_type>
                     <item_value>{ mtData._2 }</item_value>
                   </iary>
        iaryBuffer += iary
      }
      val dataElem = <datas>{ iaryBuffer }</datas>
      nodeBuffer += dataElem
      val outputXml = <EDC>{ nodeBuffer }</EDC>
      val outputFilename = xmlFile.getAbsolutePath

      xml.XML.save(
        filename = outputFilename,
        node = outputXml,
        xmlDecl = true)
      logger.info(s"write $outputFilename done")
      copyXml(eqid, glass_id)
    } catch {
      case ex: Exception =>
        logger.error("failed to output", ex)
        false
    }
  }

  def receive = {
    case ParseOutput =>
      try {
        processInputPath(parser)
      } catch {
        case ex: Throwable =>
          logger.error("processInputPath failed", ex)
      }
      import scala.concurrent.duration._
      context.system.scheduler.scheduleOnce(scala.concurrent.duration.Duration(1, scala.concurrent.duration.MINUTES), self, ParseOutput)
  }

  import java.io.File
  def parser(f: File): Boolean = {
    import java.nio.file.{ Paths, Files, StandardOpenOption }
    import java.nio.charset.{ StandardCharsets }
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
              if (dt.isDefined && !data.isEmpty)
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

      ret.foldLeft(true)((x, y) => x && y)
    }
  }

  def listAllFiles(files_path: String) = {
    //import java.io.FileFilter
    val path = new java.io.File(files_path)
    if (path.exists() && path.isDirectory()) {
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
    import java.nio.charset.{ StandardCharsets }

    val xmlPath = Paths.get(xmlOutDir.getAbsolutePath)
    val xmlFiles = files.filter(_.getName.endsWith("xml"))
    val indexFile = new File(xmlOutDir.getAbsolutePath + File.separatorChar +
      s"${DateTime.now().toString("YYYYMMdd")}_${eqid}_${glass_id}.txt")

    val alreadyInIndex =
      try {
        Files.readAllLines(Paths.get(indexFile.getAbsolutePath)).asScala.toSeq
      } catch {
        case ex: Throwable =>
          Seq.empty[String]
      }
    def appendToIndex(filePath: String) =
      Files.write(Paths.get(indexFile.getAbsolutePath), (filePath + "\r\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)

    try {
      xmlFiles.map {
        f =>
          val localPath = Paths.get(f.getAbsolutePath)
          import org.apache.commons.io._
          FileUtils.copyFileToDirectory(f, xmlOutDir, true)
          if (!alreadyInIndex.contains(f.getName))
            appendToIndex(f.getName)
          f.delete()
      }
      true
    } catch {
      case ex: Throwable =>
        logger.error(ex.toString())
        false
    }
  }

  def processInputPath(parser: (File) => Boolean) = {
    val files = listAllFiles(XmlAgent.inputPath)
    val output =
      for (f <- files) yield {
        if (f.getName.endsWith("txt")) {
          logger.info(s"parse ${f.getName}")
          try {
            val result = parser(f)

            if (result) {
              logger.info(s"${f.getAbsolutePath} success.")
              f.delete()
              1
            } else
              0
          } catch {
            case ex: Throwable =>
              logger.error("skip buggy file", ex)
              0
          }
        } else {
          f.delete()
          0
        }
      }
  }

  override def postStop = {

  }
}