defmodule EventLogtest do
  use ExUnit.Case, async: true
  alias EventLog.{Utils, Commons, Segment, Reader}
  import Utils
  import Commons

  setup do
    :ok
  end

  test "Create stream, append, read, seek" do
    dir = Path.join(temp_dir(), random_string(16))
    {:ok, log} = EventLog.start_link(dir)
    stream = "my_stream1"
    {:ok, _} = EventLog.create_stream(log, stream)
    {:ok, %{streams: ["my_stream1"]}} = EventLog.info(log)
    EventLog.append(log, stream, "foo0")
    EventLog.append(log, stream, "foo1")
    EventLog.append(log, stream, "foo2")
    EventLog.fsync(log)
    # read
    {:ok, reader} = EventLog.get_reader(log, stream)
    {:ok, {0, "foo0", _, _, _}} = Reader.get_next(reader)
    {:ok, {1, "foo1", _, _, _}} = Reader.get_next(reader)
    {:ok, {2, "foo2", _, _, _}} = Reader.get_next(reader)
    {:ok, :eof} = Reader.get_next(reader)
    # read again
    {:ok, reader} = EventLog.get_reader(log, stream)
    {:ok, {0, "foo0", _, _, _}} = Reader.get_next(reader)
    {:ok, {1, "foo1", _, _, _}} = Reader.get_next(reader)
    {:ok, {2, "foo2", _, _, _}} = Reader.get_next(reader)
    {:ok, :eof} = Reader.get_next(reader)
    # read from offset
    {:ok, reader} = EventLog.get_reader(log, stream, 2)
    {:ok, {2, "foo2", _, _, _}} = Reader.get_next(reader)
    # seek 1
    {:ok, 1} = Reader.seek(reader, 1)
    {:ok, {1, "foo1", _, _, _}} = Reader.get_next(reader)
    # seek 0
    {:ok, 0} = Reader.seek(reader, 0)
    {:ok, {0, "foo0", _, _, _}} = Reader.get_next(reader)
    # invalid offsets
    {:error, :invalid_offset} = Reader.seek(reader, 10)
    {:error, :invalid_offset} = Reader.seek(reader, -10)

    EventLog.close(log)
  end
end
