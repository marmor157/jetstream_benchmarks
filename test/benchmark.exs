parallel = [1, 2, 4, 8, 16, 32, 64, 96, 128]
tries_required = 10

defmodule MessageTimer do
  use GenServer

  def start_link do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def init(nil) do
    state = {0, nil, nil, nil}
    {:ok, state}
  end

  def handle_info({:pid, pid}, {a, b, c, _}) do
    {:noreply, {a, b, c, pid}}
  end

  def handle_info({:message, milliseconds}, {0, nil, nil, pid}) do
    {:noreply, {1, milliseconds, milliseconds, pid}}
  end

  def handle_info({:message, milliseconds}, state) do
    update_state(state, milliseconds)
    |> check_for_terminal()
  end

  defp check_for_terminal({10_00, first, last, pid}) do
    messages_per_second = 10_00 / ((last - first) / 1000.0)
    send(pid, {:finished, messages_per_second})
    {:stop, :normal, nil}
  end

  defp check_for_terminal(state), do: {:noreply, state}

  defp update_state({num_received, first, last, pid}, milliseconds) do
    cond do
      milliseconds < first ->
        {num_received + 1, milliseconds, last, pid}

      milliseconds > last ->
        {num_received + 1, first, milliseconds, pid}

      true ->
        {num_received + 1, first, last, pid}
    end
  end
end

{:ok, _} = Gnat.ConnectionSupervisor.start_link(%{name: :gnat, connection_settings: [%{}]})

# Multiple PullConsumers

defmodule ConsumeTest do
  use Jetstream.PullConsumer

  def start_link(arg) do
    Jetstream.PullConsumer.start_link(__MODULE__, arg)
  end

  @impl true
  def init(%{name: name, reply_pid: reply_pid, timer_pid: timer_pid}) do
    send(timer_pid, {:pid, reply_pid})
    {:ok, %{timer_pid: timer_pid}, connection_name: :gnat, stream_name: name, consumer_name: name}
  end

  @impl true
  def handle_message(_message, %{timer_pid: timer_pid} = state) do
    send(timer_pid, {:message, :erlang.monotonic_time(:millisecond)})
    {:ack, state}
  end
end

for paraller_size <- parallel do
  results =
    1..tries_required
    |> Enum.to_list()
    |> Enum.map(fn no_try ->
      name = "MULTIPLE_#{paraller_size}_#{no_try}"

      {:ok, _stream} =
        Jetstream.API.Stream.create(:gnat, %Jetstream.API.Stream{
          name: name,
          storage: :memory,
          subjects: ["test"]
        })

      Enum.each(1..10_00, fn i ->
        Gnat.pub(:gnat, "test", "{\"num:\": #{i}}")
      end)

      {:ok, timer_pid} = MessageTimer.start_link()

      {:ok, _consumer} =
        Jetstream.API.Consumer.create(:gnat, %Jetstream.API.Consumer{
          stream_name: name,
          durable_name: name
        })

      Enum.map(1..paraller_size, fn _ ->
        {:ok, _pid} =
          ConsumeTest.start_link(%{name: name, reply_pid: self(), timer_pid: timer_pid})
      end)

      result =
        receive do
          {:finished, msg} ->
            msg
        end

      :ok = Jetstream.API.Consumer.delete(:gnat, name, name)
      :ok = Jetstream.API.Stream.delete(:gnat, name)

      result
    end)

  average = results |> Enum.reduce(&(&1 + &2)) |> then(&(&1 / length(results)))

  IO.puts("Multiple Paraller: #{paraller_size} Average Rate: #{Float.round(average, 2)}")
end

# Multiple messages one Pull Consumer

defmodule BatchedConsumeTest do
  use Jetstream.BatchedPullConsumer

  def start_link(arg) do
    Jetstream.BatchedPullConsumer.start_link(__MODULE__, arg)
  end

  @impl true
  def init(%{name: name, reply_pid: reply_pid, timer_pid: timer_pid, batch_size: batch_size}) do
    send(timer_pid, {:pid, reply_pid})

    {:ok, %{timer_pid: timer_pid, batch_size: batch_size},
     connection_name: :gnat, stream_name: name, consumer_name: name}
  end

  @impl true
  def handle_message(_message, %{timer_pid: timer_pid} = state) do
    send(timer_pid, {:message, :erlang.monotonic_time(:millisecond)})
    {:ack, state}
  end
end

for paraller_size <- parallel do
  results =
    1..tries_required
    |> Enum.to_list()
    |> Enum.map(fn no_try ->
      name = "BATCHED_#{paraller_size}_#{no_try}"

      {:ok, _stream} =
        Jetstream.API.Stream.create(:gnat, %Jetstream.API.Stream{
          name: name,
          storage: :memory,
          subjects: ["test"]
        })

      Enum.each(1..10_00, fn i ->
        Gnat.pub(:gnat, "test", "{\"num:\": #{i}}")
      end)

      {:ok, timer_pid} = MessageTimer.start_link()

      {:ok, _consumer} =
        Jetstream.API.Consumer.create(:gnat, %Jetstream.API.Consumer{
          stream_name: name,
          durable_name: name
        })

      {:ok, _pid} =
        BatchedConsumeTest.start_link(%{
          name: name,
          reply_pid: self(),
          timer_pid: timer_pid,
          batch_size: paraller_size
        })

      result =
        receive do
          {:finished, msg} ->
            msg
        end

      :ok = Jetstream.API.Consumer.delete(:gnat, name, name)
      :ok = Jetstream.API.Stream.delete(:gnat, name)

      result
    end)

  average = results |> Enum.reduce(&(&1 + &2)) |> then(&(&1 / length(results)))

  IO.puts("Batched: #{paraller_size} Average Rate: #{Float.round(average, 2)}")
end
