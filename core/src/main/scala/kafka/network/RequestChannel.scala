/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.network

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.HashMap
import java.util.concurrent._

import com.yammer.metrics.core.Gauge
import kafka.api._
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{Logging, SystemTime}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Protocol, SecurityProtocol}
import org.apache.kafka.common.requests.{AbstractRequest, ApiVersionsRequest, ProduceRequest, RequestHeader, RequestSend}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.log4j.Logger


object RequestChannel extends Logging {
  val AllDone = new Request(processor = 1, connectionId = "2", new Session(KafkaPrincipal.ANONYMOUS, InetAddress.getLocalHost()), buffer = getShutdownReceive(), startTimeMs = 0, securityProtocol = SecurityProtocol.PLAINTEXT)


  def getShutdownReceive() = {
    val emptyRequestHeader = new RequestHeader(ApiKeys.PRODUCE.id, "", 0)
    val emptyProduceRequest = new ProduceRequest(0, 0, new HashMap[TopicPartition, ByteBuffer]())
    RequestSend.serialize(emptyRequestHeader, emptyProduceRequest.toStruct)
  }

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress)

  case class Request(processor: Int, connectionId: String, session: Session, private var buffer: ByteBuffer, startTimeMs: Long, securityProtocol: SecurityProtocol) {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    @volatile var requestDequeueTimeMs = -1L
    // @volatile 保证字段 线程 可见性 需要存在跨线程的比较 和修改
    @volatile var apiLocalCompleteTimeMs = -1L
    @volatile var responseCompleteTimeMs = -1L
    @volatile var responseDequeueTimeMs = -1L
    @volatile var apiRemoteCompleteTimeMs = -1L

    val requestId = buffer.getShort() // 请求类型ID

    // TODO: this will be removed once we migrated to client-side format
    // for server-side request / response format
    // NOTE: this map only includes the server-side request/response handlers. Newer
    // request types should only use the client-side versions which are parsed with
    // o.a.k.common.requests.AbstractRequest.getRequest()
    /**
      * 创建 服务端特有的两个Api 处理形式 （ApiKeys.FETCH.id  and  ApiKeys.CONTROLLED_SHUTDOWN_KEY.id）
      * 添加的map 在 requestObj中调用 获取到指定的 readFrom(buffer) 方法读取 request
      */
    private val keyToNameAndDeserializerMap: Map[Short, (ByteBuffer) => RequestOrResponse] =
      Map(ApiKeys.FETCH.id -> FetchRequest.readFrom,
        ApiKeys.CONTROLLED_SHUTDOWN_KEY.id -> ControlledShutdownRequest.readFrom
      )

    // TODO: this will be removed once we migrated to client-side format
    val requestObj =
      keyToNameAndDeserializerMap.get(requestId).map(readFrom => readFrom(buffer)).orNull

    // if we failed to find a server-side mapping, then try using the
    // client-side request / response format
    /**
      * 请求端 获取 请求头 先验证 是否服务端特有的 api
      */
    val header: RequestHeader = // 请求头
      if (requestObj == null) {
        buffer.rewind
        // 解析到对应的请求头 然后读取对应的 Struct schema read(buffer)
        try RequestHeader.parse(buffer)
        catch {
          case ex: Throwable =>
            throw new InvalidRequestException(s"Error parsing request header. Our best guess of the apiKey is: $requestId", ex)
        }
      } else
        null
    // 代表着各种不同的协议类型 向上集成 AbstractRequest -> AbstractRequestResponse 包含 struct 数据结构 属性 value 类型
    val body: AbstractRequest = // 请求体
      if (requestObj == null)
        try {
          // For unsupported version of ApiVersionsRequest, create a dummy request to enable an error response to be returned later
          // 对于是版本请求的api 检查其版本号 查看 是够过期 或者 超出范围
          if (header.apiKey == ApiKeys.API_VERSIONS.id && !Protocol.apiVersionSupported(header.apiKey, header.apiVersion))
            new ApiVersionsRequest
          else
          // 解析请求体
            AbstractRequest.getRequest(header.apiKey, header.apiVersion, buffer)
        } catch {
          case ex: Throwable =>
            throw new InvalidRequestException(s"Error getting request for apiKey: ${header.apiKey} and apiVersion: ${header.apiVersion}", ex)
        }
      else
        null
    // 读取 完成 buffer 置空
    buffer = null
    private val requestLogger = Logger.getLogger("kafka.request.logger")

    // toString request内容
    def requestDesc(details: Boolean): String = {
      if (requestObj != null)
        requestObj.describe(details)
      else
        header.toString + " -- " + body.toString
    }

    trace("Processor %d received request : %s".format(processor, requestDesc(true)))

    // 更新 监控时间设置 暂时 不做分析 和 实现
    def updateRequestMetrics() {
      val endTimeMs = SystemTime.milliseconds
      // In some corner cases, apiLocalCompleteTimeMs may not be set when the request completes if the remote
      // processing time is really small. This value is set in KafkaApis from a request handling thread.
      // This may be read in a network thread before the actual update happens in KafkaApis which will cause us to
      // see a negative value here. In that case, use responseCompleteTimeMs as apiLocalCompleteTimeMs.
      if (apiLocalCompleteTimeMs < 0)
        apiLocalCompleteTimeMs = responseCompleteTimeMs
      // If the apiRemoteCompleteTimeMs is not set (i.e., for requests that do not go through a purgatory), then it is
      // the same as responseCompleteTimeMs.
      if (apiRemoteCompleteTimeMs < 0)
        apiRemoteCompleteTimeMs = responseCompleteTimeMs

      val requestQueueTime = math.max(requestDequeueTimeMs - startTimeMs, 0)
      val apiLocalTime = math.max(apiLocalCompleteTimeMs - requestDequeueTimeMs, 0)
      val apiRemoteTime = math.max(apiRemoteCompleteTimeMs - apiLocalCompleteTimeMs, 0)
      val apiThrottleTime = math.max(responseCompleteTimeMs - apiRemoteCompleteTimeMs, 0)
      val responseQueueTime = math.max(responseDequeueTimeMs - responseCompleteTimeMs, 0)
      val responseSendTime = math.max(endTimeMs - responseDequeueTimeMs, 0)
      val totalTime = endTimeMs - startTimeMs
      val fetchMetricNames =
        if (requestId == ApiKeys.FETCH.id) {
          val isFromFollower = requestObj.asInstanceOf[FetchRequest].isFromFollower
          Seq(
            if (isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          )
        }
        else Seq.empty
      val metricNames = fetchMetricNames :+ ApiKeys.forId(requestId).name
      metricNames.foreach { metricName =>
        val m = RequestMetrics.metricsMap(metricName)
        m.requestRate.mark()
        m.requestQueueTimeHist.update(requestQueueTime)
        m.localTimeHist.update(apiLocalTime)
        m.remoteTimeHist.update(apiRemoteTime)
        m.throttleTimeHist.update(apiThrottleTime)
        m.responseQueueTimeHist.update(responseQueueTime)
        m.responseSendTimeHist.update(responseSendTime)
        m.totalTimeHist.update(totalTime)
      }

      if (requestLogger.isTraceEnabled)
        requestLogger.trace("Completed request:%s from connection %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d,securityProtocol:%s,principal:%s"
          .format(requestDesc(true), connectionId, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime, securityProtocol, session.principal))
      else if (requestLogger.isDebugEnabled)
        requestLogger.debug("Completed request:%s from connection %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d,securityProtocol:%s,principal:%s"
          .format(requestDesc(false), connectionId, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime, securityProtocol, session.principal))
    }
  }

  /**
    * 这个 时间  request.responseCompleteTimeMs = SystemTime.milliseconds 暂时 不用管 、
    * 简单的定义了 单个构造函数
    *
    * @param processor      处理发送 接收任务 processor Id
    * @param request        接收到的参数
    * @param responseSend   需要发送值
    * @param responseAction 接收的规则
    */
  case class Response(processor: Int, request: Request, responseSend: Send, responseAction: ResponseAction) {
    request.responseCompleteTimeMs = SystemTime.milliseconds

    def this(processor: Int, request: Request, responseSend: Send) =
      this(processor, request, responseSend, if (responseSend == null) NoOpAction else SendAction)

    def this(request: Request, send: Send) =
      this(request.processor, request, send)
  }
// 返回的三种状态值  发送动作 无响应动作 关闭连接动作
  trait ResponseAction
  // 类似于枚举
  case object SendAction extends ResponseAction
  // 设置枚举值
  case object NoOpAction extends ResponseAction

  case object CloseConnectionAction extends ResponseAction

}

class RequestChannel(val numProcessors: Int, val queueSize: Int) extends KafkaMetricsGroup {
  private var responseListeners: List[(Int) => Unit] = Nil
  // 存放listeners 主要是响应时唤醒对应Processor线程
  private val requestQueue = new ArrayBlockingQueue[RequestChannel.Request](queueSize)
  // 建立对应 processors 个 queue 存储响应信息
  private val responseQueues = new Array[BlockingQueue[RequestChannel.Response]](numProcessors)
  // 初始化 LinkedBlockingQueue<RequestChannel.Response>
  for (i <- 0 until numProcessors)
    responseQueues(i) = new LinkedBlockingQueue[RequestChannel.Response]()
  // 监控信息 暂时不用管
  newGauge(
    "RequestQueueSize",
    new Gauge[Int] {
      def value = requestQueue.size
    }
  )

  newGauge("ResponseQueueSize", new Gauge[Int] {
    def value = responseQueues.foldLeft(0) { (total, q) => total + q.size() }
  })

  for (i <- 0 until numProcessors) {
    newGauge("ResponseQueueSize",
      new Gauge[Int] {
        def value = responseQueues(i).size()
      },
      Map("processor" -> i.toString)
    )
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request
    * 阻塞发送Request
    * */
  def sendRequest(request: RequestChannel.Request) {
    requestQueue.put(request)
  }

  /** Send a response back to the socket server to be sent over the network */
  def sendResponse(response: RequestChannel.Response) {
    responseQueues(response.processor).put(response) //response.processor processor对应的ResponseQueue
    // 唤醒 对应的Processor 处理响应
    for (onResponse <- responseListeners)
    // 底层的监听 实际上 就是 唤醒 阻塞的Processor
      onResponse(response.processor)
  }

  /** No operation to take for the request, need to read more over the network
    * 新建 没有实体消息的 Response 需要自己创建 应当每个需要给与响应
    *
    * */
  def noOperation(processor: Int, request: RequestChannel.Request) {
    responseQueues(processor).put(new RequestChannel.Response(processor, request, null, RequestChannel.NoOpAction))
    // 唤醒对应的 process
    for (onResponse <- responseListeners)
      onResponse(processor)
  }

  /** Close the connection for the request */
  def closeConnection(processor: Int, request: RequestChannel.Request) {
    responseQueues(processor).put(new RequestChannel.Response(processor, request, null, RequestChannel.CloseConnectionAction))
    for (onResponse <- responseListeners)
      onResponse(processor)
  }

  /** Get the next request or block until specified time has elapsed
    * 添加拉取超时时间 防止卡死
    * */
  def receiveRequest(timeout: Long): RequestChannel.Request =
    requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /** Get the next request or block until there is one
    * 阻塞拉取请求
    * */
  def receiveRequest(): RequestChannel.Request =
    requestQueue.take()

  /** Get a response for the given processor if there is one */
  def receiveResponse(processor: Int): RequestChannel.Response = {
    val response = responseQueues(processor).poll() // 每次拉取一个Response
    if (response != null)
      response.request.responseDequeueTimeMs = SystemTime.milliseconds
    response
  }

  def addResponseListener(onResponse: Int => Unit) {
    responseListeners ::= onResponse
  }

  def shutdown() {
    requestQueue.clear
  }
}

object RequestMetrics {
  val metricsMap = new scala.collection.mutable.HashMap[String, RequestMetrics]
  val consumerFetchMetricName = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName = ApiKeys.FETCH.name + "Follower"
  (ApiKeys.values().toList.map(e => e.name)
    ++ List(consumerFetchMetricName, followFetchMetricName)).foreach(name => metricsMap.put(name, new RequestMetrics(name)))
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {
  val tags = Map("request" -> name)
  val requestRate = newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags)
  // time a request spent in a request queue
  val requestQueueTimeHist = newHistogram("RequestQueueTimeMs", biased = true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = newHistogram("LocalTimeMs", biased = true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram("RemoteTimeMs", biased = true, tags)
  // time a request is throttled (only relevant to fetch and produce requests)
  val throttleTimeHist = newHistogram("ThrottleTimeMs", biased = true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist = newHistogram("ResponseQueueTimeMs", biased = true, tags)
  // time to send the response to the requester
  val responseSendTimeHist = newHistogram("ResponseSendTimeMs", biased = true, tags)
  val totalTimeHist = newHistogram("TotalTimeMs", biased = true, tags)
}

