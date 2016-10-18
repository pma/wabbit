defmodule Wabbit.Basic do
  @moduledoc """
  Functions to publish, consume and acknowledge messages.
  """

  require Record
  import Wabbit.Record

  Record.defrecordp :amqp_msg, [props: p_basic(), payload: ""]

  @doc """
  Publishes a message to an Exchange.

  This method publishes a message to a specific exchange. The message will be routed
  to queues as defined by the exchange configuration and distributed to any subscribers.

  The parameter `exchange` specifies the name of the exchange to publish to. If set to
  empty string, it publishes to the default exchange.
  The `routing_key` parameter specifies the routing key for the message.

  The `payload` parameter specifies the message content as a binary.

  In addition to the previous parameters, the following options can be used:

  # Options

    * `:mandatory` - If set, returns an error if the broker can't route the message to a queue (default `false`);
    * `:immediate` - If set, returns an error if the broker can't deliver te message to a consumer immediately (default `false`);
    * `:content_type` - MIME Content type;
    * `:content_encoding` - MIME Content encoding;
    * `:headers` - Message headers. Can be used with headers Exchanges;
    * `:persistent` - If set, uses persistent delivery mode. Messages marked as `persistent` that are delivered to `durable` \
                      queues will be logged to disk;
    * `:correlation_id` - application correlation identifier;
    * `:priority` - message priority, ranging from 0 to 9;
    * `:reply_to` - name of the reply queue;
    * `:expiration` - how long the message is valid (in milliseconds);
    * `:message_id` - message identifier;
    * `:timestamp` - timestamp associated with this message (epoch time);
    * `:type` - message type as a string;
    * `:user_id` - creating user ID. RabbitMQ will validate this against the active connection user;
    * `:app_id` - publishing application ID.

  ## Examples

      iex> Wabbit.Basic.publish channel, \"Hello World!\", exchange: \"my_exchange\", routing_key: \"my_routing_key\", persistent: true
      :ok

  """
  def publish(channel, payload, options \\ []) do
    basic_publish =
      basic_publish(exchange:    Keyword.get(options, :exchange, ""),
                    routing_key: Keyword.get(options, :routing_key, ""),
                    mandatory:   Keyword.get(options, :mandatory, false),
                    immediate:   Keyword.get(options, :immediate, false))
    p_basic =
      p_basic(content_type:     Keyword.get(options, :content_type,     :undefined),
              content_encoding: Keyword.get(options, :content_encoding, :undefined),
              headers:          Keyword.get(options, :headers,          :undefined), # |> Utils.to_type_tuple,
              delivery_mode:    if(options[:persistent], do: 2, else: 1),
              priority:         Keyword.get(options, :priority,         :undefined),
              correlation_id:   Keyword.get(options, :correlation_id,   :undefined),
              reply_to:         Keyword.get(options, :reply_to,         :undefined),
              expiration:       Keyword.get(options, :expiration,       :undefined),
              message_id:       Keyword.get(options, :message_id,       :undefined),
              timestamp:        Keyword.get(options, :timestamp,        :undefined),
              type:             Keyword.get(options, :type,             :undefined),
              user_id:          Keyword.get(options, :user_id,          :undefined),
              app_id:           Keyword.get(options, :app_id,           :undefined),
              cluster_id:       Keyword.get(options, :cluster_id,       :undefined))

    :amqp_channel.cast(channel, basic_publish, amqp_msg(props: p_basic, payload: payload))
  end

  @doc """
  Acknowledges one or more messages. If `multiple` is set to `true`, all messages up to the one
  specified by `delivery_tag` are considered acknowledged by the server.
  """
  def ack(channel, delivery_tag, options \\ []) do
    :amqp_channel.call(channel,
      basic_ack(delivery_tag: delivery_tag,
                multiple: Keyword.get(options, :multiple, false)))
  end

  @doc """
  Rejects (and, optionally, requeues) a message.
  """
  def reject(channel, delivery_tag, options \\ []) do
    :amqp_channel.call(channel,
      basic_reject(delivery_tag: delivery_tag,
                   requeue: Keyword.get(options, :requeue, true)))
  end

  @doc """
  Negative acknowledge of one or more messages. If `multiple` is set to `true`, all messages up to the
  one specified by `delivery_tag` are considered as not acknowledged by the server. If `requeue` is set
  to `true`, the message will be returned to the queue and redelivered to the next available consumer.
  This is a RabbitMQ specific extension to AMQP 0.9.1. It is equivalent to reject, but allows rejecting
  multiple messages using the `multiple` option.
  """
  def nack(channel, delivery_tag, options \\ []) do
    :amqp_channel.call(channel,
      basic_nack(delivery_tag: delivery_tag,
                 multiple: Keyword.get(options, :multiple, false),
                 requeue: Keyword.get(options, :requeue, true)))
  end

  @doc """
  Registers a queue consumer process. The `pid` of the process can be set using
  the `subscriber` argument and defaults to the calling process.
  The consumer process will receive the following data structures:
  * `{:basic_deliver, payload, meta}` - This is sent for each message consumed, where \
  `payload` contains the message content and `meta` contains all the metadata set when \
  sending with Basic.publish or additional info set by the broker;
  * `{:basic_consume_ok, %{consumer_tag: consumer_tag}}` - Sent when the consumer \
  process is registered with Basic.consume. The caller receives the same information \
  as the return of Basic.consume;
  * `{:basic_cancel, %{consumer_tag: consumer_tag, no_wait: no_wait}}` - Sent by the \
  broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
  * `{:basic_cancel_ok, %{consumer_tag: consumer_tag}}` - Sent to the consumer process after a call to Basic.cancel
  """
  def consume(channel, queue, options \\ []) do
    basic_consume =
      basic_consume(
        queue: queue,
        consumer_tag: Keyword.get(options, :consumer_tag, ""),
        no_local:     Keyword.get(options, :no_local,     false),
        no_ack:       Keyword.get(options, :no_ack,       false),
        exclusive:    Keyword.get(options, :exclusive,    false),
        nowait:       Keyword.get(options, :no_wait,      false),
        arguments:    Keyword.get(options, :arguments,    []))

    basic_consume_ok(consumer_tag: consumer_tag) =
      :amqp_channel.subscribe(channel, basic_consume, Keyword.get(options, :subscriber, self()))

    {:ok, consumer_tag}
  end

  @doc """
  Sets the message prefetch count or prefetech size (in bytes). If `global` is set to `true` this
  applies to the entire Connection, otherwise it applies only to the specified Channel.
  """
  def qos(channel, options \\ []) do
    basic_qos_ok() = :amqp_channel.call(channel,
      basic_qos(
        prefetch_size:  Keyword.get(options, :prefetch_size,  0),
        prefetch_count: Keyword.get(options, :prefetch_count, 0),
        global:         Keyword.get(options, :global,         false)))
    :ok
  end

end
