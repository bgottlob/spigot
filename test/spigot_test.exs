defmodule SpigotTest do
  use ExUnit.Case
  doctest Spigot

  test "greets the world" do
    assert Spigot.hello() == :world
  end
end
