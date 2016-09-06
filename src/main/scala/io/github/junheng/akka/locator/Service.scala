package io.github.junheng.akka.locator

import akka.actor.{PoisonPill, Actor, ActorLogging}

trait Service extends Actor with ActorLogging {

  private var instance = ServiceLocator.createServiceInstance(self, context.system, 0.0, Service.STATUS_NORMAL)

  private val monitor = context.actorOf(LoadMonitor.props(self, this))

  override def preStart(): Unit = {
    super.preStart()
    ServiceLocator.discovery.registerService(instance)
    ServiceLocator.locals.put(instance.getName, self)
    ServiceLocator.instances.put(instance.getName, instance)
    log.info(s"service registered ${instance.getName} - ${instance.getId} - ${new String(instance.getPayload.url)}")
  }

  override def postStop(): Unit = {
    super.postStop()
    monitor ! PoisonPill
    ServiceLocator.discovery.unregisterService(instance)
    ServiceLocator.locals.remove(instance.getName)
    ServiceLocator.instances.remove(instance.getName)
    log.info(s"service quited: ${new String(instance.getPayload.url)}")
  }

  def reportLoad(load: Double, status: String = Service.STATUS_NORMAL) = {
    if (instance.getPayload.load != load || instance.getPayload.status != status) {
      instance = ServiceLocator.createServiceInstance(self, context.system, load, status)
      ServiceLocator.discovery.updateService(instance)
      ServiceLocator.instances.put(instance.getName, instance)
      log.info(s"overload $load status $status")
    }
  }

}

object Service {
  val TYPE_ACTOR = "actor"
  val STATUS_NORMAL = "normal"
  val STATUS_OVERLOAD = "overload"
}


