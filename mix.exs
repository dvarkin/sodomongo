defmodule Sodo.Mixfile do
  use Mix.Project

  def project do
    [app: :sodomongo,
     version: "0.0.1",
     compilers: [:erlang, :app],
     deps: deps]
  end

  def application do
    [applications: [:kernel,
                    :stdlib,
                    :inets,
                    :crypto,
                    :protobuffs,
                    :eredis,
                    :folsom,
                    :folsomite],
    mod: {:sodomongo_app, []}
    ]
  end
  defp deps do
    [{:folsom, "~> 0.8.3"},
     {:folsomite, "~> 1.2"},
     {:protobuffs, "~> 0.8.4"},
     {:eredis, "~> 1.0"},
     {:rethinkdb, "~> 0.4.0"},
     {:jsone, "~> 1.4.0"},
     {:uuid, "~> 1.1"}
     ]
  end
end
