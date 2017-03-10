defmodule Wabbit.Mixfile do
  use Mix.Project

  def project do
    [app: :wabbit,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  def application do
    [applications: [:logger, :amqp_client, :connection, :gen_stage,
                    :asn1, :public_key, :ssl],
     mod: {Wabbit, []}]
  end

  defp deps do
    [{:gen_stage, "~> 0.11"},
     {:connection, "~> 1.0"},
     {:amqp_client, github: "jbrisbin/amqp_client"},
     {:rabbit_common, github: "jbrisbin/rabbit_common", override: true}]
  end
end
