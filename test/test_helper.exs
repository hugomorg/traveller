Application.ensure_all_started(:postgrex)
{:ok, _} = Traveller.TestRepo.start_link()
ExUnit.start()
