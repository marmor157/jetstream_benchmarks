defmodule Jetstream.BatchedPullConsumer.Job do
  def run(message, module, listening_topic, state) do
    case module.handle_message(message, state) do
      {:ack, _state} ->
        Jetstream.ack_next(message, listening_topic)
    end
  end
end
