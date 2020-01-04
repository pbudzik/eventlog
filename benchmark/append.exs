defmodule EventLog.Benchmark do
  import EventLog.Utils
  import EventLog.Commons
  require Logger

  def bench_append(n, size) do
    dir = Path.join(temp_dir(), random_string(16))
    {:ok, log} = EventLog.start_link(dir)
    stream = "my_stream1"
    {:ok, _} = EventLog.create_stream(log, stream)

    message = random_string(size)
    t0 = :os.system_time(:millisecond)
    range = 1..n
    volume = size * n

    for i <- range do
      EventLog.append(log, stream, message)
    end

    EventLog.close(log)
    elapsed = :os.system_time(:millisecond) - t0
    t = volume / elapsed * 1000 / 1_000_000
    mps = round(n / elapsed * 1000)

    IO.puts(
      "#{n} messages of #{size}b size => elapsed: #{elapsed} ms, mps: #{mps} msg/s, throughput: #{t} MB/s"
    )
  end
end

Logger.configure(level: :error)

EventLog.Benchmark.bench_append(10_000, 128)
EventLog.Benchmark.bench_append(10_000, 512)
EventLog.Benchmark.bench_append(10_000, 1024)
