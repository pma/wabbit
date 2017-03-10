# GenStage Pipeline:
#   Producer -> Sink -> [RabbitMQ Broker] -> Source -> Consumer

defmodule Producer do
  use GenStage

  def start_link() do
    GenStage.start_link(__MODULE__, 0)
  end

  def init(counter) do
    {:producer, counter}
  end

  def handle_demand(demand, counter) when demand > 0 do
    events = Enum.to_list(counter..counter+demand-1)
    {:noreply, events, counter + demand}
  end
end

defmodule Sink do
  use Wabbit.GenStage

  def start_link(conn) do
    Wabbit.GenStage.start_link(__MODULE__, conn, :ok, name: __MODULE__)
  end

  def init(:ok) do
    {:consumer, 0, max_unconfirmed: 1000}
  end

  def handle_channel_opened(chan, state) do
    # Declare exchange, queue, bindings, etc...
    {:ok, %{queue: queue}} = Wabbit.Queue.declare(chan, "command_handler", durable: true)
    # Set default publish options: use default exchange and route directly to queue
    {:ok, state, exchange: "", routing_key: queue, persistent: true}
  end

  def handle_encode(event, state) do
    # Encode event to binary payload and override default publish options
    payload = Integer.to_string(event)
    publish_opts = [timestamp: :os.system_time()]
    {:ok, payload, state + 1, publish_opts}
  end
end

defmodule Source do
  use Wabbit.GenStage

  def start_link(conn) do
    Wabbit.GenStage.start_link(__MODULE__, conn, :ok, name: __MODULE__)
  end

  def init(:ok) do
    {:producer, 0}
  end

  def handle_channel_opened(chan, state) do
    # Declare exchange, queue, bindings, etc...
    {:ok, %{queue: queue}} = Wabbit.Queue.declare(chan, "command_handler", durable: true)
    # Set consume queue and options
    {:ok, queue, state, prefetch_count: 1000}
  end

  def handle_decode(payload, meta, state) do
    case Integer.parse(payload) do
      {event, ""} ->
        {:ok, event, state + 1}
      _ ->
        :ok = reject(meta.channel, meta.delivery_tag, requeue: false)
        {:error, :invalid_message, state}
    end
  end

  defp reject(channel, delivery_tag, opts) do
    try do
      Wabbit.Basic.reject(channel, delivery_tag, opts)
    catch
      _, _ -> :ok
    end
  end
end

defmodule Consumer do
  use GenStage

  def start_link() do
    GenStage.start_link(__MODULE__, :ok)
  end

  def init(:ok) do
    {:consumer, :ok}
  end

  def handle_events(events, _from, state) do
    for {event, meta} <- events do
      IO.inspect {event, meta}
      :ok = ack(meta.channel, meta.delivery_tag)
    end
    {:noreply, [], state}
  end

  defp ack(channel, delivery_tag) do
    try do
      Wabbit.Basic.ack(channel, delivery_tag)
    catch
      _, _ ->
        :ok
    end
  end
end

{:ok, producer} = Producer.start_link
{:ok, consumer} = Consumer.start_link

{:ok, conn}   = Wabbit.Connection.start_link(host: "localhost", port: 5672, username: "guest", password: "guest")

{:ok, sink}   = Sink.start_link(conn)
{:ok, source} = Source.start_link(conn)

GenStage.sync_subscribe(sink, to: producer, min_demand: 500, max_demand: 1000)
GenStage.sync_subscribe(consumer, to: source, max_demand: 1000)
