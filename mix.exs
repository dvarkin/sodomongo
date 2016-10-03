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
                   # :zeta,
                    :eredis,
                    :folsom,
                    :folsomite,]]
  end
  defp deps do
    [{:folsom, "~> 0.8.3"},
     {:mongodb, github: "dvarkin/mongodb-erlang", tag: "master"},
     {:folsomite, "~> 1.2"},
     {:protobuffs, "~> 0.8.4"},
     #{:bson, "~> 0.4.4"},
     {:eredis, "~> 1.0"},
     {:rethinkdb, "~> 0.4.0"}
     #{:zeta, github: "tel/zeta", tag: "master"}
     ]
  end
end