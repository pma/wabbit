defmodule Wabbit.Queue do
  @moduledoc """
  Functions to operate on Queues.
  """

  import Wabbit.Record

  alias Wabbit.Utils

  @doc """
  Declares a queue.

  The optional `queue` parameter is used to set the name. If set to an
  empty string (default), the server will assign a name. Besides the
  queue name, the following options can be used:

  # Options

    * `:durable` - If set, keeps the Queue between restarts of the broker
    * `:auto_delete` - If set, deletes the Queue once all subscribers disconnect
    * `:exclusive` - If set, only one subscriber can consume from the Queue
    * `:passive` - If set, raises an error unless the queue already exists
    * `:no_wait` - If set, the server will not respond to the method
    * `:arguments` - A set of arguments for the declaration
  """
  def declare(channel, queue \\ "", options \\ []) do
    queue_declare =
      queue_declare(queue:       queue,
                    passive:     Keyword.get(options, :passive,     false),
                    durable:     Keyword.get(options, :durable,     false),
                    exclusive:   Keyword.get(options, :exclusive,   false),
                    auto_delete: Keyword.get(options, :auto_delete, false),
                    nowait:      Keyword.get(options, :no_wait,     false),
                    arguments:   Keyword.get(options, :arguments,   []) |> Utils.to_type_tuple)
    queue_declare_ok(queue:          queue,
                     message_count:  message_count,
                     consumer_count: consumer_count) = :amqp_channel.call(channel, queue_declare)
    {:ok, %{queue: queue, message_count: message_count, consumer_count: consumer_count}}
  end

  @doc """
  Binds a Queue to an Exchange

  The following options can be used:

  # Options

    * `:routing_key` - If set, specifies the routing key for the binding
    * `:no_wait` - If set, the server will not respond to the method
    * `:arguments` - A set of arguments for the binding
  """
  def bind(channel, queue, exchange, options \\ []) do
    queue_bind =
      queue_bind(queue:       queue,
                 exchange:    exchange,
                 routing_key: Keyword.get(options, :routing_key, ""),
                 nowait:      Keyword.get(options, :no_wait,     false),
                 arguments:   Keyword.get(options, :arguments,   []) |> Utils.to_type_tuple)
    queue_bind_ok() = :amqp_channel.call(channel, queue_bind)
    :ok
  end

  @doc """
  Unbinds a Queue from an Exchange

  The following options can be used:

  # Options

    * `:routing_key` - If set, specifies the routing key for the unbind
    * `:arguments` - A set of arguments for the unbind
  """
  def unbind(channel, queue, exchange, options \\ []) do
    queue_unbind =
      queue_unbind(queue:       queue,
                   exchange:    exchange,
                   routing_key: Keyword.get(options, :routing_key, ""),
                   arguments:   Keyword.get(options, :arguments,   []))
    queue_unbind_ok() = :amqp_channel.call(channel, queue_unbind)
    :ok
  end

  @doc """
  Deletes a Queue by name

  The following options can be used:

  # Options

    * `:if_unused` - If set, the server will only delete the queue if it has no consumers
    * `:if_empty` - If set, the server will only delete the queue if it has no messages
    * `:no_wait` - If set, the server will not respond to the method
  """
  def delete(channel, queue, options \\ []) do
    queue_delete =
      queue_delete(queue:     queue,
                   if_unused: Keyword.get(options, :if_unused, false),
                   if_empty:  Keyword.get(options, :if_empty,  false),
                   nowait:    Keyword.get(options, :no_wait,   false))
    queue_delete_ok(message_count: message_count) = :amqp_channel.call(channel, queue_delete)
    {:ok, %{message_count: message_count}}
  end

  @doc """
  Discards all messages in the Queue
  """
  def purge(channel, queue) do
    queue_purge_ok(message_count: message_count) = :amqp_channel.call(channel, queue_purge(queue: queue))
    {:ok, %{message_count: message_count}}
  end

  @doc """
  Returns the message count and consumer count for the given queue.

  Uses Queue.declare with the `passive` option set.
  """
  def status(channel, queue) do
    declare(channel, queue, passive: true)
  end

  @doc """
  Returns the number of messages that are ready for delivery (e.g. not
  pending acknowledgements) in the queue
  """
  def message_count(channel, queue) do
    {:ok, %{message_count: message_count}} = status(channel, queue)
    message_count
  end

  @doc """
  Returns a number of active consumers on the queue
  """
  def consumer_count(channel, queue) do
    {:ok, %{consumer_count: consumer_count}} = status(channel, queue)
    consumer_count
  end

  @doc """
  Returns true if queue is empty (has no messages ready), false otherwise
  """
  def empty?(channel, queue) do
    message_count(channel, queue) == 0
  end
end
