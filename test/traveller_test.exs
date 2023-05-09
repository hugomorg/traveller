defmodule TravellerTest do
  use ExUnit.Case
  doctest Traveller

  test "greets the world" do
    assert Traveller.hello() == :world
  end
end
