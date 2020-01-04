defmodule LogTest do
  use ExUnit.Case, async: true
  alias EventLog.{Utils, Commons, Appender, Reader, Segment}
  import Utils
  import Commons

  setup do
    :ok
  end

  def test_dir(), do: Path.join(temp_dir(), random_string(16))

  test "Basic append + read" do
    dir = test_dir()
    {:ok, appender} = Appender.start_link(dir)
    {:ok, o1} = Appender.append(appender, "ABCD")
    assert o1 == 0
    {:ok, o2} = Appender.append(appender, "2")
    assert o2 == 1
    {:ok, o3} = Appender.append(appender, "3")
    assert o3 == 2
    Appender.close(appender)
    IO.inspect(File.ls!(dir))
    assert Segment.segments(dir) |> Enum.count() == 1
    file = Segment.segments(dir) |> List.first()
    stat = File.stat!("#{dir}/#{file}")
    assert stat.size == 17 * 3 + (4 + 1 + 1)
    {:ok, reader} = Reader.start_link(dir)
    {:ok, {off1, v1, ts1, _, _}} = Reader.get_next(reader)
    assert off1 == 0
    assert v1 == "ABCD"
    {:ok, {off2, v2, ts2, _, _}} = Reader.get_next(reader)
    assert off2 == 1
    assert v2 == "2"
    {:ok, {off3, v3, ts3, _, _}} = Reader.get_next(reader)
    assert off3 == 2
    assert v3 == "3"
    assert ts3 >= ts2 and ts2 >= ts1 and ts1 > 0
    Reader.close(reader)
    File.rm_rf!(dir)
  end

  test "Segment splitting: 1500 bytes needs 2 segs of 1000" do
    dir = test_dir()
    {:ok, appender} = Appender.start_link(dir, max_seg_size: 1000)
    Appender.append(appender, random_string(500))
    Appender.append(appender, random_string(500))
    Appender.append(appender, random_string(500))
    assert Segment.segments(dir) |> Enum.count() == 2
    Appender.close(appender)
    {:ok, reader} = Reader.start_link(dir)
    {:ok, _} = Reader.get_next(reader)
    {:ok, _} = Reader.get_next(reader)
    {:ok, _} = Reader.get_next(reader)
    {:ok, :eof} = Reader.get_next(reader)
    Reader.close(reader)
    File.rm_rf!(dir)
  end

  test "Bulk append followed by bulk read and seek" do
    dir = test_dir()
    {:ok, appender} = Appender.start_link(dir, max_seg_size: 100)

    for i <- 0..999 do
      Appender.append(appender, "value#{i}")
    end

    Appender.close(appender)

    {:ok, reader} = Reader.start_link(dir)

    for i <- 0..999 do
      {:ok, {offset, value, _, _, _}} = Reader.get_next(reader)
      assert offset == i
      assert value == "value#{i}"
    end

    {:ok, {10, "value10", _, _, _}} = Reader.get_one(reader, 10)
    {:ok, {100, "value100", _, _, _}} = Reader.get_one(reader, 100)
    {:ok, {999, "value999", _, _, _}} = Reader.get_one(reader, 999)
    {:ok, {0, "value0", _, _, _}} = Reader.get_one(reader, 0)
    {:ok, {0, "value0", _, _, _}} = Reader.get_one(reader, 0)

    entries = Reader.get_batch(reader, 0, 1000)
    assert Enum.count(entries) == 1000

    Reader.close(reader)

    IO.inspect(File.ls!(dir) |> Enum.count())
    File.rm_rf!(dir)
  end

  test "Reopen" do
    dir = test_dir()
    {:ok, appender} = Appender.start_link(dir)
    {:ok, 0} = Appender.append(appender, "0")
    {:ok, 1} = Appender.append(appender, "1")
    Appender.close(appender)
    # reopend and append
    {:ok, appender} = Appender.start_link(dir)
    {:ok, 2} = Appender.append(appender, "2")
    Appender.close(appender)

    {:ok, reader} = Reader.start_link(dir)
    {:ok, {0, "0", _, _, _}} = Reader.get_next(reader)
    {:ok, {1, "1", _, _, _}} = Reader.get_next(reader)
    {:ok, {2, "2", _, _, _}} = Reader.get_next(reader)
    Reader.close(reader)
    File.rm_rf!(dir)
  end
end
