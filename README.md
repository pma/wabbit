# Wabbit

GenStage based interface for RabbitMQ.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add `wabbit` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:wabbit, "~> 0.1.0"}]
    end
    ```

  2. Ensure `wabbit` is started before your application:

    ```elixir
    def application do
      [applications: [:wabbit]]
    end
    ```

  3. Optional - configure via `config/config.exs` and friends; this support also works well with Distillery

    ```elixir
    use Mix.Config

    config :wabbit,
      host: "localhost",
      port: 5672,
      username: "guest",
      password: "guest"
    ```
