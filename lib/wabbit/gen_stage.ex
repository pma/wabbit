defmodule Wabbit.GenStage do
  import Wabbit.Record
  require Record
  Record.defrecordp :amqp_msg, [props: p_basic(), payload: ""]

  defstruct [:mod, :state, :type, :demand, :consumer_tag, :table_id,
             :conn, :chan, :queue, :unconfirmed, :max_unconfirmed,
             producers: %{}, publish_options: []]

  @typedoc "The supported stage types."
  @type type :: :producer | :consumer

  @typedoc "The supported init options"
  @type options :: []

  @typedoc "The supported consume options"
  @type publish_options :: []

  @typedoc "The supported consume options"
  @type consume_options :: []

  @publish_options ~w(
    exchange   routing_key mandatory immediate      content_type content_encoding
    headers    persistent  priority  correlation_id reply_to     expiration
    message_id timestamp   type      user_id        app_id       cluster_id
  )a

  @consume_options ~w(
    prefetch_size  prefetch_count  global     consumer_tag
    no_local       no_ack          exclusive  no_wait
    arguments
  )a

  @callback init(args :: term) ::
    {type, state} |
    {type, state, options} |
    :ignore |
    {:stop, reason :: any} when state: any

  @callback handle_channel_opened(channel :: pid, state :: term) ::
    {:ok, new_state} |
    {:ok, new_state, publish_options} |
    {:ok, queue, new_state} |
    {:ok, queue, new_state, consume_options} when new_state: term, queue: binary

  @callback handle_encode(message :: term, state :: term) ::
    {:ok, payload, new_state} |
    {:ok, payload, new_state, publish_options} |
    {:error, reason, new_state :: any} when new_state: term, reason: term, payload: binary

  @callback handle_decode(payload :: binary, meta :: map, state :: term) ::
    {:ok, message :: any, new_state} |
    {:error, reason, new_state :: any} when new_state: term, reason: term

  @callback handle_demand(demand :: pos_integer, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, :hibernate} |
    {:stop, reason, new_state} when new_state: term, reason: term, event: term

  @callback handle_subscribe(:producer | :consumer, opts :: [options],
                             to_or_from :: GenServer.from, state :: term) ::
    {:automatic | :manual, new_state} |
    {:stop, reason, new_state} when new_state: term, reason: term

  @callback handle_cancel({:cancel | :down, reason :: term}, GenServer.from, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, :hibernate} |
    {:stop, reason, new_state} when event: term, new_state: term, reason: term

  @callback handle_events([event], GenServer.from, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, :hibernate} |
    {:stop, reason, new_state} when new_state: term, reason: term, event: term

  @callback handle_call(request :: term, GenServer.from, state :: term) ::
    {:reply, reply, [event], new_state} |
    {:reply, reply, [event], new_state, :hibernate} |
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term, event: term

  @callback handle_cast(request :: term, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term, event: term

  @callback handle_info(msg :: term, state :: term) ::
    {:noreply, [event], new_state} |
    {:noreply, [event], new_state, :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term, event: term

  @callback terminate(reason, state :: term) ::
    term when reason: :normal | :shutdown | {:shutdown, term} | term

  @callback code_change(old_vsn, state :: term, extra :: term) ::
    {:ok, new_state :: term} |
    {:error, reason :: term} when old_vsn: term | {:down, term}

  @callback format_status(:normal | :terminate, [pdict :: {term, term} | state :: term, ...]) ::
    status :: term

  @optional_callbacks [handle_demand: 2, handle_events: 3, format_status: 2]

  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      @behaviour Wabbit.GenStage

      @doc false
      def handle_channel_opened(channel, %{type: :consumer} = state) do
        {:ok, state}
      end

      @doc false
      def handle_channel_opened(channel, %{type: :producer} = state) do
        {:ok, %{queue: queue}} =
          Wabbit.Queue.declare(channel, "", auto_delete: true)
        {:ok, queue, state}
      end

      @doc false
      def handle_encode(message, state) do
        {:ok, inspect(message), state}
      end

      @doc false
      def handle_decode(payload, _meta, state) do
        {:ok, payload, state}
      end

      @doc false
      def handle_call(msg, _from, state) do
        # We do this to trick Dialyzer to not complain about non-local returns.
        reason = {:bad_call, msg}
        case :erlang.phash2(1, 1) do
          0 -> exit(reason)
          1 -> {:stop, reason, state}
        end
      end

      @doc false
      def handle_info(_msg, state) do
        {:noreply, [], state}
      end

      @doc false
      def handle_subscribe(_kind, _opts, _from, state) do
        {:automatic, state}
      end

      @doc false
      def handle_cancel(_reason, _from, state) do
        {:noreply, [], state}
      end

      @doc false
      def handle_cast(msg, state) do
        # We do this to trick Dialyzer to not complain about non-local returns.
        reason = {:bad_cast, msg}
        case :erlang.phash2(1, 1) do
          0 -> exit(reason)
          1 -> {:stop, reason, state}
        end
      end

      @doc false
      def terminate(_reason, _state) do
        :ok
      end

      @doc false
      def code_change(_old, state, _extra) do
        {:ok, state}
      end

      defoverridable [handle_channel_opened: 2, handle_encode: 2, handle_decode: 3,
                      handle_call: 3, handle_info: 2, handle_cast: 2,
                      handle_subscribe: 4, handle_cancel: 3, terminate: 2, code_change: 3]
    end
  end

  def start_link(module, connection, args, options \\ []) when is_atom(module) and is_list(options) do
    GenStage.start_link(__MODULE__, {module, connection, args}, options)
  end

  def start(module, connection, args, options \\ []) when is_atom(module) and is_list(options) do
    GenStage.start(__MODULE__, {module, connection, args}, options)
  end

  def init({mod, conn, args}) do
    case mod.init(args) do
      {:consumer, state} ->
        init_consumer(mod, conn, [], state)
      {:consumer, state, opts} ->
        init_consumer(mod, conn, opts, state)
      {:producer, state} ->
        init_producer(mod, conn, [], state)
      {:producer, state, opts} ->
        init_producer(mod, conn, opts, state)
      {:stop, _} = stop ->
        stop
      :ignore ->
        :ignore
      other ->
        {:stop, {:bad_return_value, other}}
    end
  end

  defp init_consumer(mod, conn, opts, state) do
    {max_unconfirmed, opts} = Keyword.pop(opts, :max_unconfirmed)
    {publish_opts, opts} = Keyword.split(opts, @publish_options)
    {:consumer, open_channel(
        %Wabbit.GenStage{
          type: :consumer,
          mod: mod,
          producers: %{},
          table_id: :ets.new(:unconfirmed, [:ordered_set, :private]),
          unconfirmed: 0,
          max_unconfirmed: max_unconfirmed,
          queue: :queue.new(),
          publish_options: publish_opts,
          state: state,
          conn: conn,
          chan: nil}), opts}
  end

  defp init_producer(mod, conn, opts, state) do
    {:producer, open_channel(
        %Wabbit.GenStage{
          type: :producer,
          mod: mod,
          queue: :queue.new(),
          demand: 0,
          conn: conn,
          chan: nil,
          state: state}), opts}
  end

  def handle_demand(incoming_demand, state) do
    dispatch_events(state, incoming_demand + state.demand, [])
  end

  def handle_subscribe(:producer, opts, from, state) do
    max = pending = opts[:max_demand] || 1_000
    min = opts[:min_demand] || 500

    # Register the producer in the state
    state = %{state | producers: Map.put(state.producers, from, {pending, min, max})}

    # Ask for the pending events
    state = ask(state, from)

    # Returns manual as we want control over the demand
    {:manual, state}
  end

  def handle_subscribe(_kind, _opts, _from, state) do
    {:automatic, state}
  end

  def handle_cancel(_, from, state) do
    # Remove the producers from the map on unsubscribe
    {:noreply, [], %{state | producers: Map.delete(state.producers, from)}}
  end

  def handle_events(events, from, state) do
    {:noreply, [], Enum.reduce(events, state, &publish_or_enqueue(&1, from, &2))}
  end

  def handle_call(msg, from, %{mod: mod, state: mod_state} = state) do
    case mod.handle_call(msg, from, mod_state) do
      {:reply, reply, mod_state}  ->
        {:reply, reply, [], %{state | state: mod_state}}
      {:reply, reply, mod_state, :hibernate} ->
        {:reply, reply, [], %{state | state: mod_state}, :hibernate}
      {:stop, reason, reply, mod_state} ->
        {:stop, reason, reply, %{state | state: mod_state}}
      return ->
        handle_noreply_callback(return, state)
    end
  end

  def handle_cast(msg, %{state: mod_state} = state) do
    noreply_callback(:handle_cast, [msg, mod_state], state)
  end

  def handle_info({:"basic.ack", seqno, multiple}, state) do
    state = state |> confirm(seqno, multiple) |> ask()
    {:noreply, [], state}
  end

  def handle_info({:DOWN, _monitor_ref, :process, _pid, _}, %{type: :consumer} = state) do
    {:noreply, [], open_channel_and_flush(state)}
  end

  def handle_info({:DOWN, _monitor_ref, :process, _pid, _}, %{type: :producer} = state) do
    {:noreply, [], open_channel(state)}
  end

  def handle_info(:open_channel, %{type: :consumer} = state) do
    {:noreply, [], open_channel_and_flush(state)}
  end

  def handle_info(:open_channel, %{type: :producer} = state) do
    {:noreply, [], open_channel(state)}
  end

  def handle_info(basic_consume_ok(consumer_tag: consumer_tag), state) do
    {:noreply, [], %{state | consumer_tag: consumer_tag}}
  end

  def handle_info({
    basic_deliver(consumer_tag: consumer_tag,
                  delivery_tag: delivery_tag,
                  redelivered: redelivered,
                  exchange: exchange,
                  routing_key: routing_key),
    amqp_msg(props: p_basic(content_type: content_type,
                            content_encoding: content_encoding,
                            headers: headers,
                            delivery_mode: delivery_mode,
                            priority: priority,
                            correlation_id: correlation_id,
                            reply_to: reply_to,
                            expiration: expiration,
                            message_id: message_id,
                            timestamp: timestamp,
                            type: type,
                            user_id: user_id,
                            app_id: app_id,
                            cluster_id: cluster_id), payload: payload)}, state) do
    meta = %{consumer_tag: consumer_tag,
             delivery_tag: delivery_tag,
             redelivered: redelivered,
             exchange: exchange,
             routing_key: routing_key,
             content_type: content_type,
             content_encoding: content_encoding,
             headers: headers,
             persistent: delivery_mode == 2,
             priority: priority,
             correlation_id: correlation_id,
             reply_to: reply_to,
             expiration: expiration,
             message_id: message_id,
             timestamp: timestamp,
             type: type,
             user_id: user_id,
             app_id: app_id,
             cluster_id: cluster_id,
             channel: state.chan}

    case state.mod.handle_decode(payload, meta, state.state) do
      {:ok, message, new_state} ->
        state = %{state | queue: :queue.in({message, meta}, state.queue), state: new_state}
        dispatch_events(state, state.demand, [])
      {:error, reason, new_state} ->
        :error_logger.format("Decode error with reason: ~s~n", [reason])
        {:noreply, [], %{state | state: new_state}}
    end
  end

  def handle_info(msg, %{state: mod_state} = state) do
    noreply_callback(:handle_info, [msg, mod_state], state)
  end

  @doc false
  def terminate(reason, %{mod: mod, state: mod_state}) do
    mod.terminate(reason, mod_state)
  end

  @doc false
  def code_change(old_vsn, %{module: module, state: mod_state} = state, extra) do
    case module.code_change(old_vsn, mod_state, extra) do
      {:ok, mod_state} -> {:ok, %{state | state: mod_state}}
      other -> other
    end
  end

  defp open_channel(%{type: type} = state) do
    case Wabbit.Connection.open_channel(state.conn) do
      {:ok, chan} ->
        case state.mod.handle_channel_opened(chan, state.state) do
          {:ok, queue, new_state} when type == :producer ->
            set_channel_and_start_consuming(state, chan, queue, new_state, [])
          {:ok, queue, new_state, opts} when type == :producer ->
            set_channel_and_start_consuming(state, chan, queue, new_state, opts)
          {:ok, new_state} when type == :consumer ->
            set_channel_and_prepare_publish(state, chan, new_state, [])
          {:ok, new_state, opts} when type == :consumer ->
            set_channel_and_prepare_publish(state, chan, new_state, opts)
          other ->
            {:stop, {:bad_return_value, other}}
        end
      _ ->
        :erlang.send_after(:timer.seconds(1), self(), :open_channel)
        %{state | chan: nil}
    end
  end

  defp set_channel_and_start_consuming(state, chan, queue, new_state, opts) do
    _monitor_ref = Process.monitor(chan)
    :ok = Wabbit.Basic.qos(chan, opts)
    {:ok, consumer_tag} = Wabbit.Basic.consume(chan, queue, opts)
    %{state | chan: chan, consumer_tag: consumer_tag, state: new_state}
  end

  defp dispatch_events(state, 0, events) do
    {:noreply, Enum.reverse(events), state}
  end
  defp dispatch_events(state, demand, events) do
    case :queue.out(state.queue) do
      {{:value, event}, queue} ->
        dispatch_events(%{state | queue: queue}, demand - 1, [event | events])
      {:empty, _} ->
        {:noreply, Enum.reverse(events), %{state | demand: demand}}
    end
  end

  defp noreply_callback(callback, args, %{module: module} = state) do
    handle_noreply_callback apply(module, callback, args), state
  end

  defp handle_noreply_callback(return, state) do
    case return do
      {:noreply, mod_state} ->
        {:noreply, [], %{state | state: mod_state}}
      {:noreply, mod_state, :hibernate} ->
        {:noreply, [], %{state | state: mod_state}, :hibernate}
      {:stop, reason, mod_state} ->
        {:stop, reason, %{state | state: mod_state}}
      other ->
        {:stop, {:bad_return_value, other}, state}
    end
  end

  defp set_channel_and_prepare_publish(state, chan, new_state, publish_opts) do
    _monitor_ref = Process.monitor(chan)
    confirm_select_ok() = :amqp_channel.call(chan, confirm_select())
    :ok = :amqp_channel.register_confirm_handler(chan, self())
    %{state | chan: chan, state: new_state, publish_options: publish_opts}
  end

  defp open_channel_and_flush(state) do
    case open_channel(state) do
      %{chan: nil} = state ->
        state
      state ->
        state |> flush() |> ask()
    end
  end

  defp publish_or_enqueue(event, from, %{chan: nil} = state) do
    %{state | queue: :queue.in({from, event}, state.queue)}
  end

  defp publish_or_enqueue(event, from, state) do
    case state.mod.handle_encode(event, state.state) do
      {:ok, payload, new_state} ->
        publish_or_enqueue(state, event, payload, from, new_state, [])
      {:ok, payload, new_state, publish_opts} ->
        publish_or_enqueue(state, event, payload, from, new_state, publish_opts)
      {:error, reason, new_state} ->
        :error_logger.format("Encode error with reason: ~s~n", [reason])
        {:noreply, [], %{state | state: new_state}}
    end
  end

  defp publish_or_enqueue(state, event, payload, from, new_state, publish_opts) do
    publish_opts = Keyword.merge(state.publish_options, publish_opts)
    with {:ok, seqno} <- next_publish_seqno(state.chan),
         :ok <- Wabbit.Basic.publish(state.chan, payload, publish_opts),
         true <- :ets.insert(state.table_id, {seqno, {from, event}}) do
      %{state | state: new_state, unconfirmed: state.unconfirmed + 1}
    else
      {:error, reason} ->
        :error_logger.format("Publish error with reason: ~s~n", [reason])
        %{state | state: new_state, queue: :queue.in({from, event}, state.queue)}
    end
  end

  defp next_publish_seqno(channel) do
    try do
      {:ok, :amqp_channel.next_publish_seqno(channel)}
    catch
      :exit, reason ->
        {:error, reason}
    end
  end

  defp drain_confirm_acks(state) do
    receive do
      {:"basic.ack", seqno, multiple} ->
        state |> confirm(seqno, multiple) |> drain_confirm_acks()
    after
      0 -> %{state | unconfirmed: 0}
    end
  end

  defp flush(state) do
    state |> drain_confirm_acks() |> flush_unconfirmed() |> flush_queue()
  end

  defp flush_queue(state) do
    %{state | queue: :queue.new()} |> flush_queue(state.queue)
  end

  defp flush_queue(state, queue) do
    case :queue.out(queue) do
      {:empty, _} ->
        state
      {{:value, {from, event}}, queue} ->
        publish_or_enqueue(event, from, state) |> flush_queue(queue)
    end
  end

  defp flush_unconfirmed(state) do
    pending_confirm = :ets.select(state.table_id, [{{:"$1", :"$2"}, [], [:"$2"]}])

    true = :ets.delete_all_objects(state.table_id)

    Enum.reduce(pending_confirm, state, fn {from, event}, state ->
      publish_or_enqueue(event, from, state)
    end)
  end

  defp confirm(state, seqno, multiple) do
    ms =
      case multiple do
        true ->
          [{{:"$1", :"$2"}, [{:"=<", :"$1", {:const, seqno}}], [{{:"$1", :"$2"}}]}]
        false ->
          [{{:"$1", :"$2"}, [{:"==", :"$1", {:const, seqno}}], [{{:"$1", :"$2"}}]}]
      end

    {producers, unconfirmed} =
      Enum.reduce(:ets.select(state.table_id, ms), {state.producers, state.unconfirmed},
        fn {seqno, {from, _event}}, {producers, unconfirmed} ->
          true = :ets.delete(state.table_id, seqno)
          producers = Map.update!(producers, from, fn {pending, min, max} ->
            {pending + 1, min, max}
          end)
          {producers, unconfirmed - 1}
        end)

    %{state | producers: producers, unconfirmed: unconfirmed}
  end

  defp ask(%{chan: nil} = state, _from),
    do: state
  defp ask(%{unconfirmed: unconfirmed,
             max_unconfirmed: max_unconfirmed} = state, _) when unconfirmed > max_unconfirmed,
    do: state
  defp ask(state, from) do
    case state.producers do
      %{^from => {pending, min, max}} when pending > min ->
        GenStage.ask(from, pending)
        %{state | producers: Map.put(state.producers, from, {0, min, max})}
      %{} ->
        state
    end
  end

  defp ask(%{chan: nil} = state),
    do: state
  defp ask(%{unconfirmed: unconfirmed, max_unconfirmed: max_unconfirmed} = state)
  when unconfirmed > max_unconfirmed,
    do: state
  defp ask(state) do
    Enum.reduce(state.producers, state, fn {from, _}, state ->
      ask(state, from)
    end)
  end
end
