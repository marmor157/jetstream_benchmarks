parallel = [1, 2, 4, 8, 16, 32, 64, 96, 128]
tries_required = 10

defmodule Message do
  @message "[{\"_id\":\"626285d30788417cba0020fd\",\"index\":0,\"guid\":\"52dd9f4f-5abc-4477-aab3-6ff25e23227a\",\"isActive\":true,\"balance\":\"$3,583.26\",\"picture\":\"http://placehold.it/32x32\",\"age\":24,\"eyeColor\":\"blue\",\"name\":\"Dunlap Whitehead\",\"gender\":\"male\",\"company\":\"MICRONAUT\",\"email\":\"dunlapwhitehead@micronaut.com\",\"phone\":\"+1 (811) 520-2248\",\"address\":\"514 Hemlock Street, Echo, Illinois, 7628\",\"about\":\"Qui irure irure sit mollit eu exercitation Lorem ad id. Cillum eu commodo enim ullamco minim excepteur exercitation dolor. Tempor ullamco culpa quis proident ipsum ut consequat sunt quis cillum minim incididunt. Mollit exercitation adipisicing Lorem eu. Et dolore in irure proident.\\r\\n\",\"registered\":\"2017-01-22T10:53:22 -01:00\",\"latitude\":-18.585929,\"longitude\":117.11434,\"tags\":[\"nisi\",\"non\",\"consequat\",\"fugiat\",\"cupidatat\",\"nostrud\",\"tempor\"],\"friends\":[{\"id\":0,\"name\":\"Keisha Hayes\"},{\"id\":1,\"name\":\"Hawkins Berry\"},{\"id\":2,\"name\":\"Jami Glover\"}],\"greeting\":\"Hello, Dunlap Whitehead! You have 8 unread messages.\",\"favoriteFruit\":\"apple\"},{\"_id\":\"626285d3040ee1ae7d71587e\",\"index\":1,\"guid\":\"d2b8fd12-3935-4a36-b7bd-535900beb7e3\",\"isActive\":true,\"balance\":\"$1,547.55\",\"picture\":\"http://placehold.it/32x32\",\"age\":21,\"eyeColor\":\"green\",\"name\":\"Nina Summers\",\"gender\":\"female\",\"company\":\"SHADEASE\",\"email\":\"ninasummers@shadease.com\",\"phone\":\"+1 (932) 542-2038\",\"address\":\"128 Cobek Court, Aurora, North Dakota, 3234\",\"about\":\"Et nulla enim commodo cupidatat ea non est. Mollit reprehenderit magna adipisicing duis enim. Ipsum veniam commodo anim sunt proident aliqua exercitation nulla ipsum pariatur adipisicing. Commodo do magna eiusmod non pariatur adipisicing id. Laborum ipsum magna aute exercitation qui pariatur dolore. Occaecat nisi sunt est occaecat aliqua voluptate nostrud quis minim velit aliqua cillum elit quis. Et ad mollit dolore nulla.\\r\\n\",\"registered\":\"2014-09-19T02:07:24 -02:00\",\"latitude\":-19.63547,\"longitude\":-62.735062,\"tags\":[\"pariatur\",\"exercitation\",\"sint\",\"nulla\",\"ullamco\",\"cillum\",\"voluptate\"],\"friends\":[{\"id\":0,\"name\":\"Guerra Burns\"},{\"id\":1,\"name\":\"Roberson Waller\"},{\"id\":2,\"name\":\"Nell Spencer\"}],\"greeting\":\"Hello, Nina Summers! You have 3 unread messages.\",\"favoriteFruit\":\"banana\"},{\"_id\":\"626285d397124032d5bad842\",\"index\":2,\"guid\":\"52d90018-7201-4036-b908-d3b347e1677a\",\"isActive\":false,\"balance\":\"$1,587.06\",\"picture\":\"http://placehold.it/32x32\",\"age\":20,\"eyeColor\":\"brown\",\"name\":\"Hillary Dawson\",\"gender\":\"female\",\"company\":\"KAGE\",\"email\":\"hillarydawson@kage.com\",\"phone\":\"+1 (952) 459-3514\",\"address\":\"258 Division Avenue, Rushford, Georgia, 2032\",\"about\":\"Officia anim labore dolore culpa adipisicing. Cupidatat ad Lorem incididunt nisi ea mollit aliqua ut. Incididunt qui reprehenderit adipisicing ad anim veniam voluptate cillum proident elit. Laboris quis ea consectetur ullamco. Lorem irure ut commodo excepteur deserunt do qui sunt sit esse adipisicing. Consequat ex amet Lorem id elit ullamco deserunt proident non eu anim cillum veniam. Voluptate elit commodo eu ullamco.\\r\\n\",\"registered\":\"2017-09-20T04:50:26 -02:00\",\"latitude\":-34.081225,\"longitude\":-101.288151,\"tags\":[\"non\",\"eiusmod\",\"do\",\"ad\",\"veniam\",\"non\",\"et\"],\"friends\":[{\"id\":0,\"name\":\"Tessa Howell\"},{\"id\":1,\"name\":\"Mueller Church\"},{\"id\":2,\"name\":\"Marci Peterson\"}],\"greeting\":\"Hello, Hillary Dawson! You have 7 unread messages.\",\"favoriteFruit\":\"banana\"},{\"_id\":\"626285d3f37bc1e70467d8ef\",\"index\":3,\"guid\":\"1ff4ee2f-eb1c-4abd-834f-27578741b1b1\",\"isActive\":true,\"balance\":\"$1,569.43\",\"picture\":\"http://placehold.it/32x32\",\"age\":22,\"eyeColor\":\"brown\",\"name\":\"Pruitt Emerson\",\"gender\":\"male\",\"company\":\"KOZGENE\",\"email\":\"pruittemerson@kozgene.com\",\"phone\":\"+1 (812) 463-2886\",\"address\":\"743 Abbey Court, Thornport, Idaho, 1089\",\"about\":\"Aliquip duis cillum tempor irure sint voluptate. Pariatur sunt officia laboris voluptate ea eiusmod reprehenderit adipisicing qui. Quis ex aute laboris fugiat Lorem consectetur anim in excepteur deserunt pariatur qui sint in. Veniam culpa elit sunt tempor. Ea ullamco cillum elit sint deserunt sit. Quis labore fugiat laboris amet qui est fugiat consequat nostrud id ullamco nulla proident consequat.\\r\\n\",\"registered\":\"2021-09-21T02:49:06 -02:00\",\"latitude\":-88.619956,\"longitude\":1.692127,\"tags\":[\"laboris\",\"Lorem\",\"adipisicing\",\"nostrud\",\"laboris\",\"incididunt\",\"aute\"],\"friends\":[{\"id\":0,\"name\":\"Stacie Mccarthy\"},{\"id\":1,\"name\":\"Hampton Wood\"},{\"id\":2,\"name\":\"Eileen Rocha\"}],\"greeting\":\"Hello, Pruitt Emerson! You have 6 unread messages.\",\"favoriteFruit\":\"apple\"},{\"_id\":\"626285d337a17799e127fd86\",\"index\":4,\"guid\":\"b8454cdc-3a15-4a13-8939-6b1294543991\",\"isActive\":false,\"balance\":\"$3,470.67\",\"picture\":\"http://placehold.it/32x32\",\"age\":33,\"eyeColor\":\"blue\",\"name\":\"Chrystal Robertson\",\"gender\":\"female\",\"company\":\"QUALITEX\",\"email\":\"chrystalrobertson@qualitex.com\",\"phone\":\"+1 (945) 429-3499\",\"address\":\"838 Brooklyn Avenue, Somerset, Montana, 2486\",\"about\":\"Nostrud ipsum commodo nostrud ut anim est do magna veniam laborum adipisicing. Eiusmod qui do adipisicing do nostrud minim dolore esse laboris quis fugiat laboris laboris. Amet consectetur ullamco sit laborum anim cupidatat eu aliquip duis irure excepteur.\\r\\n\",\"registered\":\"2019-03-26T09:52:33 -01:00\",\"latitude\":49.619732,\"longitude\":83.277563,\"tags\":[\"velit\",\"dolore\",\"ipsum\",\"elit\",\"ex\",\"aliqua\",\"quis\"],\"friends\":[{\"id\":0,\"name\":\"Ana Nielsen\"},{\"id\":1,\"name\":\"Greene Kirkland\"},{\"id\":2,\"name\":\"Jody Reyes\"}],\"greeting\":\"Hello, Chrystal Robertson! You have 8 unread messages.\",\"favoriteFruit\":\"apple\"}]"

  def get() do
    @message
  end
end

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

  defp check_for_terminal({10_000, first, last, pid}) do
    messages_per_second = 10_000 / ((last - first) / 1000.0)
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

defmodule ParallerScenario do
  @behaviour Beamchmark.Scenario

  @impl true
  def run() do
    parallel = [1, 2, 4, 8, 16, 32, 64, 96, 128]
    tries_required = 10

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

          Enum.each(1..10_000, fn i ->
            Gnat.pub(:gnat, "test", Message.get())
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
  end
end

defmodule BatchScenario do
  @behaviour Beamchmark.Scenario

  @impl true
  def run() do
    parallel = [1, 2, 4, 8, 16, 32, 64, 96, 128]
    tries_required = 10

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
              subjects: ["batched"]
            })

          Enum.each(1..10_000, fn i ->
            Gnat.pub(:gnat, "batched", Message.get())
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
  end
end

# Beamchmark.run(ParallerScenario,
#   delay: 1,
#   duration: 30,
#   formatters: [
#     Beamchmark.Formatters.Console,
#     {Beamchmark.Formatters.HTML,
#      [output_path: "reports/beamchmark.html", auto_open?: true, inline_assets?: true]}
#   ]
# )

# Beamchmark.run(BatchScenario,
#   delay: 1,
#   duration: 30,
#   formatters: [
#     Beamchmark.Formatters.Console,
#     {Beamchmark.Formatters.HTML,
#      [output_path: "reports/beamchmark.html", auto_open?: true, inline_assets?: true]}
#   ]
# )
ParallerScenario.run()
BatchScenario.run()
