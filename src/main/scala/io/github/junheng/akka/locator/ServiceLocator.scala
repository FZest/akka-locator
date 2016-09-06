package io.github.junheng.akka.locator

import java.util.concurrent.TimeUnit

import akka.TransportExtension
import akka.actor._
import akka.util.Timeout
import org.apache.commons.codec.digest.DigestUtils
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.curator.framework.state.ConnectionState.RECONNECTED
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryForever
import org.apache.curator.x.discovery.{ServiceDiscovery, ServiceDiscoveryBuilder, ServiceInstance}
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.NoNodeException
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object ServiceLocator {

  private val log = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  var curator: CuratorFramework = null
  var discovery: ServiceDiscovery[ServiceLocation] = null
  val locals = mutable.Map.empty[String, ActorRef]
  val instances = mutable.Map.empty[String, ServiceInstance[ServiceLocation]]

  def initialize(zookeeper: String, namespace: String = "dragon") = {
    curator = CuratorFrameworkFactory.builder()
      .namespace(namespace)
      .connectString(zookeeper)
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(1000))
      .build()

    discovery = ServiceDiscoveryBuilder.builder(classOf[ServiceLocation])
      .client(ServiceLocator.curator)
      .basePath("/")
      .build()

    curator.start()

    var esNodePath: String = null

    curator.getConnectionStateListenable.addListener(new ConnectionStateListener() {
      override def stateChanged(curator: CuratorFramework, newState: ConnectionState): Unit = {
        log.debug("zookeeper state is changed to " + newState.name().toLowerCase)
        if (RECONNECTED == newState) {
          if (null != esNodePath) {
            Try(curator.delete().forPath(esNodePath)) match {
              case Success(_) | Failure(_: NoNodeException) => esNodePath = null
              case Failure(ex: Throwable) => log.error(s"fail to delete node $esNodePath in zookeeper $zookeeper when reconnect")
            }
          }
          Try(esNodePath = curator.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/node")) match {
            case Failure(ex) => log.error(s"fail to create node $esNodePath in zookeeper $zookeeper when reconnect", ex)
            case Success(_) =>
          }
          instances.values.foreach { instance =>
            Try(discovery.registerService(instance)) match {
              case Failure(ex) => log.error(s"fail to register service ${instance.getName} in zookeeper $zookeeper when reconnect", ex)
              case Success(_) =>
            }
          }
        }
      }
    })

    if (curator.blockUntilConnected(10, TimeUnit.SECONDS)) {
      esNodePath = curator.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/node")
    } else new RuntimeException(s"can not connect to zookeeper: $zookeeper")

  }

  def hex(buf: Array[Byte]): String = buf.map("%02X" format _).mkString

  def register(actorRef: ActorRef, system: ActorSystem) = {
    ServiceLocator.discovery.registerService(createServiceInstance(actorRef, system, 0.0, Service.STATUS_NORMAL))
  }

  def update(actorRef: ActorRef, system: ActorSystem, load: Double, status: String): Unit = {
    ServiceLocator.discovery.updateService(createServiceInstance(actorRef, system, load, status))
  }

  def createServiceInstance(actorRef: ActorRef, system: ActorSystem, load: Double, status: String): ServiceInstance[ServiceLocation] = {
    val name = s"${actorRef.path.elements.tail.mkString("-")}"

    val remote = actorRef.path.toStringWithAddress(TransportExtension(system).address)

    val identity = hex(DigestUtils.md5(remote))

    val instance = ServiceInstance.builder[ServiceLocation]()
      .payload(new ServiceLocation(Service.TYPE_ACTOR, remote, load, status))
      .name(name)
      .id(identity)
      .build()
    instance
  }
}


trait ServiceLocator {
  this: Actor with ActorLogging =>
  implicit val timeout = Timeout(10 seconds)

  def located(path: String): Located = new LocatedService(path)(log)
}


