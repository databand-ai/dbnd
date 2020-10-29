package ai.databand

import java.net.{HttpURLConnection, URL}

import ai.databand.annotations.Task
import ai.databand.log.DbndLogger
import org.slf4j.LoggerFactory

object SanityCheck {

    private val LOG = LoggerFactory.getLogger(this.getClass)

    @Task("jvm_sanity_check")
    def main(args: Array[String]): Unit = {
        val config = new DbndConfig
        LOG.info("Databand server set to {}", config.databandUrl())
        LOG.info("Probing databand url...")
        try {
            val res = get(config.databandUrl() + "/app")
            if (res.contains("<title>Databand</title>")) {
                LOG.info("Connection to databand established successfully")
            } else {
                LOG.info("Connection established, but the response is wrong")
            }
        } catch {
            case ioe: java.io.IOException => LOG.error("Unable to perform http request", ioe)
            case ste: java.net.SocketTimeoutException => LOG.info("Connection timed out", ste)
        }
        LOG.info("Creating pipeline...")
        DbndWrapper.instance().beforePipeline(
            "ai.databand.SanityCheck",
            "public static void ai.databand.SanityCheck.main(java.lang.String[])", args.toArray[AnyRef]
        )
        DbndLogger.logMetric("time", System.currentTimeMillis().toDouble)
        DbndLogger.logMetric("status", "ok")
        DbndWrapper.instance().afterPipeline()
        LOG.info("Pipeline stopped")
    }

    def get(url: String,
            connectTimeout: Int = 15000,
            readTimeout: Int = 15000,
            requestMethod: String = "GET"): String = {
        val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
        connection.setConnectTimeout(connectTimeout)
        connection.setReadTimeout(readTimeout)
        connection.setRequestMethod(requestMethod)
        val inputStream = connection.getInputStream
        val content = io.Source.fromInputStream(inputStream).mkString
        if (inputStream != null) {
            inputStream.close
        }
        content
    }
}
