defmodule TroupeGateway.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :troupe_gateway,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      # 示例路径参与编译检查（examples/*.exs 由 mix run 执行，不入包）
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto, :eex]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps, do: []

  defp description do
    "Troupe 外部端点传输桥（ADR-0007 L3）—— WebSocket 会话/有界 mailbox/Outbox 派发循环/事件去重（troupe-gateway Crystal shard 的 Elixir 实现）"
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/ShiningRay/troupe-gateway-elixir",
        "ADR-0007" =>
          "https://github.com/ShiningRay/troupe/blob/main/docs/adr/0007-external-endpoint-layering.md"
      },
      files: ~w(lib mix.exs README.md LICENSE)
    ]
  end
end
