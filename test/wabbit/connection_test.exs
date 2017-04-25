defmodule Wabbit.ConnectionTest do
  use ExUnit.Case
  alias Wabbit.Connection

  # This test assumes that RabbitMQ server is running with default settings on
  # localhost.
  test "open connection with default settings" do
    assert {:ok, conn} = Connection.start_link()
    :ok = Connection.close(conn)
  end

  test "open connection with host as binary" do
    assert {:ok, conn} = Connection.start_link(host: "localhost")
    :ok = Connection.close(conn)
  end

  test "open connection with host as charlist" do
    assert {:ok, conn} = Connection.start_link(host: 'localhost')
    :ok = Connection.close(conn)
  end

  test "open connection using uri" do
    assert {:ok, conn} = Connection.start_link("amqp://localhost")
    assert :ok = Connection.close(conn)
  end
end
