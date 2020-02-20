package io.vlingo.symbio.store.state.jdbc;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.vlingo.actors.Logger;
import io.vlingo.actors.Stage;
import io.vlingo.common.Completes;
import io.vlingo.reactivestreams.Elements;
import io.vlingo.reactivestreams.PublisherConfiguration;
import io.vlingo.reactivestreams.Sink;
import io.vlingo.reactivestreams.Source;
import io.vlingo.reactivestreams.Stream;
import io.vlingo.reactivestreams.StreamPublisher;
import io.vlingo.reactivestreams.StreamSubscriber;
import io.vlingo.reactivestreams.Streams;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.StateBundle;

public class JDBCStateStoreStream<RS> implements Stream {
  private final JDBCStorageDelegate<TextState> delegate;
  private long flowElementsRate;
  private final Logger logger;
  private Publisher<RS> publisher;
  private final Stage stage;
  private final ResultSet resultSet;
  private ResultSetSource<RS> resultSetSource;
  private final StateAdapterProvider stateAdapterProvider;
  private StateStreamSubscriber<RS> subscriber;

  public JDBCStateStoreStream(
          final Stage stage,
          final JDBCStorageDelegate<TextState> delegate,
          final StateAdapterProvider stateAdapterProvider,
          final ResultSet resultSet,
          final Logger logger) {
    this.stage = stage;
    this.delegate = delegate;
    this.stateAdapterProvider = stateAdapterProvider;
    this.resultSet = resultSet;
    this.logger = logger;
  }

  @Override
  public void request(long flowElementsRate) {
    this.flowElementsRate = flowElementsRate;

    subscriber.subscriptionHook.request(this.flowElementsRate);
  }

  @Override
  public <S> void flowInto(final Sink<S> sink) {
    flowInto(sink, DefaultFlowRate, DefaultProbeInterval);
  }

  @Override
  public <S> void flowInto(final Sink<S> sink, final long flowElementsRate) {
    flowInto(sink, flowElementsRate, DefaultProbeInterval);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <S> void flowInto(final Sink<S> sink, final long flowElementsRate, final int probeInterval) {
    this.flowElementsRate = flowElementsRate;

    final PublisherConfiguration configuration =
            PublisherConfiguration.with(
                    probeInterval,
                    Streams.DefaultMaxThrottle,
                    Streams.DefaultBufferSize,
                    Streams.OverflowPolicy.DropCurrent);

    resultSetSource = new ResultSetSource<>(resultSet, delegate, stateAdapterProvider, flowElementsRate, logger);

    publisher = stage.actorFor(Publisher.class, StreamPublisher.class, resultSetSource, configuration);

    final Subscriber<RS> subscriber =
            stage.actorFor(
                    Subscriber.class,
                    StateStreamSubscriber.class,
                    sink,
                    flowElementsRate,
                    this);

    publisher.subscribe(subscriber);
  }

  @Override
  public void stop() {
    subscriber.subscriptionHook.cancel();
  }

  private static final class ResultSetSource<RS> implements Source<RS> {
    final JDBCStorageDelegate<TextState> delegate;
    private final long flowElementsRate;
    private final Logger logger;
    private final ResultSet resultSet;
    private final StateAdapterProvider stateAdapterProvider;

    public ResultSetSource(
            final ResultSet resultSet,
            final JDBCStorageDelegate<TextState> delegate,
            final StateAdapterProvider stateAdapterProvider,
            final long flowElementsRate,
            final Logger logger) {
      this.resultSet = resultSet;
      this.delegate = delegate;
      this.stateAdapterProvider = stateAdapterProvider;
      this.flowElementsRate = flowElementsRate;
      this.logger = logger;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Completes<Elements<RS>> next() {
      try {
        if (resultSet.isClosed()) {
          return Completes.withFailure(Elements.terminated());
        }

        int count = 0;
        boolean done = false;

        final List<StateBundle> next = new ArrayList<>();

        while (count++ < flowElementsRate) {
          if (!resultSet.next()) {
            done = true;
            break;
          }
          final String id = resultSet.getString(1);
          final TextState state = delegate.stateFrom(resultSet, id, 1);
          final Object object = stateAdapterProvider.fromRaw(state);
          next.add(new StateBundle(state, object));
        }
        if (next.size() > 0) {
          final Elements elements = Elements.of(arrayFrom(next));
          return Completes.withSuccess(elements);
        }

        if (!done) {
          return Completes.withSuccess(Elements.empty());
        }
      } catch (Exception e) {
        logger.error("Failed to stream next state elements because: " + e.getMessage(), e);
      }

      try {
        resultSet.close();
      } catch (Exception e) {
        logger.error("Failed to close result set because: " + e.getMessage(), e);
      }

      return Completes.withSuccess(Elements.terminated());
    }

    @Override
    public Completes<Elements<RS>> next(final int maximumElements) {
      return next();
    }

    @Override
    public Completes<Elements<RS>> next(final long index) {
      return next();
    }

    @Override
    public Completes<Elements<RS>> next(final long index, final int maximumElements) {
      return next();
    }

    @Override
    public Completes<Boolean> isSlow() {
      return Completes.withSuccess(false);
    }

    private StateBundle[] arrayFrom(final List<StateBundle> states) {
      return states.toArray(new StateBundle[states.size()]);
    }
  }

  public static class StateStreamSubscriber<RS> extends StreamSubscriber<RS> {
    Subscription subscriptionHook;

    public StateStreamSubscriber(
            final Sink<RS> sink,
            final long requestThreshold,
            final JDBCStateStoreStream<RS> stateStream) {

      super(sink, requestThreshold);

      stateStream.subscriber = this;
    }

    @Override
    public void onComplete() {
      super.onComplete();
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      this.subscriptionHook = subscription;

      super.onSubscribe(subscription);
    }
  }
}
