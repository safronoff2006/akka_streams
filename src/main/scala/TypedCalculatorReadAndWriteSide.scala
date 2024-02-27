import akka.NotUsed
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Behavior, Props}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream._
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.{ClosedShape, FlowShape, SinkShape, SourceShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka_typed.CalculatorRepository.{createSession, getlatestOffsetAndResult, updatedResultAndOffset}
import akka_typed.TypedCalculatorWriteSide._
import slick.jdbc.GetResult

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

object akka_typed {
  trait CborSerialization
  val persId = PersistenceId.ofUniqueId("001")

  case class Result(state: Double, offset: Long)

  implicit val sess = createSession()


  object TypedCalculatorWriteSide {

    sealed trait Command
    case class Add(amount: Double) extends Command
    case class Multiply(amount: Double) extends Command
    case class Divide(amount: Double) extends Command

    sealed trait Event
    case class Added(id: Int, amount: Double) extends Event
    case class Multiplied(id: Int, amount: Double) extends Event
    case class Divided(id: Int, amount: Double) extends Event

    final case class State(value: Double) extends CborSerialization{
      def add(amount: Double): State = copy(value = value + amount)
      def multiply(amount: Double): State = copy(value = value * amount)
      def divide(amount: Double): State = copy(value = value / amount)
    }

    object State{
      val empty=State(0)
    }

    def handleCommand(
                     persId: String,
                     state: State,
                     command: Command,
                     ctx: ActorContext[Command]
                     ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"receive adding for number: $amount and state is ${state.value} ")
          val added = Added(persId.toInt, amount)
          Effect.persist(added)
          .thenRun{
            x=> ctx.log.info(s"The state result is ${x.value}")
          }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiply for number: $amount and state is ${state.value} ")
          val multiplied = Multiplied(persId.toInt, amount)
          Effect.persist(multiplied)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive divide for number: $amount and state is ${state.value} ")
          val divided = Divided(persId.toInt, amount)
          Effect.persist(divided)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handling Event added is: $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handling Event multiplied is: $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handling Event divided is: $amount and state is ${state.value}")
          state.divide(amount)
      }

    def apply(): Behavior[Command] =
      Behaviors.setup{ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }

  }


  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed]){


    implicit val materializer = system.classicSystem


    private var (state: Double, offset: Long) = Result.unapply(getlatestOffsetAndResult).getOrElse(throw new Exception("Отсутствие результата"))

    val startOffset: Long = if (offset == 1) 1 else offset + 1

    val readJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)


    val source: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("001", startOffset, Long.MaxValue)

    private def updateState(event: Any, seqNum: Long): Result =
      Result(
        event match {
          case Added(_, amount) =>
            println(s"Added - old state: $state  new state: ${state + amount}")
            state + amount
          case Multiplied(_, amount) =>
            println(s"Multiplied - old state: $state  new state: ${state + amount}")
            state * amount
          case Divided(_, amount) =>
            println(s"Divided - old state: $state  new state: ${state + amount}")
            state / amount

        },
        seqNum
      )

    val graph: Graph[ClosedShape.type, NotUsed] = GraphDSL.create(){
      builder: GraphDSL.Builder[NotUsed] =>
        val inputFromSource: SourceShape[EventEnvelope] = builder.add(source)

        val stateModifyFlow: FlowShape[EventEnvelope, Result] = builder.add(Flow[EventEnvelope].map(eventenv => updateState(eventenv.event, eventenv.sequenceNr)))

        val localSaveOutputSink: SinkShape[Result] = builder.add(Sink.foreach[Result]{
          result: Result => {
            println(s"Saved $result")
            state = result.state
          }
        })

        val dbSaveOutputSink: SinkShape[Result] = builder.add(updatedResultAndOffset)

        val broadcasting =  builder.add(Broadcast[Result](2))

        inputFromSource -> stateModifyFlow -> broadcasting

        broadcasting.out(0) -> localSaveOutputSink
        broadcasting.out(1) -> dbSaveOutputSink

        ClosedShape

    }

  }

  object CalculatorRepository {

    def createSession(): SlickSession = {
      //Конфигурация slick-postgres прописана в конфиге reference.conf
      val sessionOfSlick: SlickSession = SlickSession.forConfig("slick-postgres")
      println(s"Slick session: $sessionOfSlick")
      sessionOfSlick
    }

    implicit val implresult: GetResult[Result] = GetResult(r => Result( r.nextDouble(), r.nextLong()))

    def getlatestOffsetAndResult(implicit   sess: SlickSession): Result = {
      import sess.profile.api._
      val query: Future[Option[Result]] =  sess.db.run(sql"select calculated_value, write_side_offset from public.result where id = 1".as[Result].headOption)
      Await.result( query, 800 millis).getOrElse( throw new Exception("Отсутствие результата"))
    }

    def updatedResultAndOffset(implicit sess: SlickSession) = {
      import sess.profile.api._
      Slick.sink[Result]{
        result: Result => sqlu"update public.result set calculated_value = ${result.state}, write_side_offset = ${result.offset} where id = 1"
      }
    }

  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup{
      ctx =>
        val writeActorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
        writeActorRef ! Add(10)
        writeActorRef ! Multiply(2)
        writeActorRef ! Divide(5)
        Behaviors.same
    }

  def main(args: Array[String]): Unit = {
    val value = akka_typed()
    implicit val system: ActorSystem[NotUsed] = ActorSystem(value, "akka_typed")
    implicit  val executionContext = system.executionContext


    val graph = TypedCalculatorReadSide(system).graph
    RunnableGraph.fromGraph(graph).run()

    system.whenTerminated.onComplete(_ => sess.close())
  }


}