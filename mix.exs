defmodule Wabbit.Mixfile do
  use Mix.Project

  def project do
    [
      app: :wabbit,
      version: "0.5.0",
      elixir: "~> 1.12",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      source_url: "https://github.com/pma/wabbit",
      deps: deps()
    ]
  end

  def application do
    [
      applications: [:logger, :asn1, :public_key, :ssl, :amqp_client, :connection, :gen_stage],
      mod: {Wabbit, []}
    ]
  end

  defp deps do
    [
      {:gen_stage, "~> 1.1"},
      {:connection, "~> 1.0"},
      {:amqp_client, "~> 3.8"},
      {:rabbit_common, "~> 3.8"},
      {:ex_doc, "~> 0.17", only: :dev}
    ]
  end

  defp description do
    """
    GenStage based interface for RabbitMQ
    """
  end

  defp package do
    [
      files: ["lib", "mix.exs", "README.md", "LICENSE"],
      maintainers: ["Paulo Almeida"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/pma/wabbit"}
    ]
  end
end
