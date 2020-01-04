defmodule EventLog.Appender do
  @moduledoc """
  Appends data entries to a log.
  """
  use GenServer
  require Logger
  alias EventLog.{Segment, IndexWriter, IndexReader}

  @write_mode [:append, :binary, :delayed_write]
  @max_seg_size 1024 * 1024 * 1024

  def start_link(dir, opts \\ [max_seg_size: @max_seg_size]) do
    GenServer.start_link(__MODULE__, [dir, opts])
  end

  def init([dir, opts]) do
    Logger.info("init, opts: #{inspect(opts)}")
    Logger.debug("dir: #{dir}, opts: #{inspect(opts)}")

    File.mkdir_p!(dir)

    {seg_path, seg_size, offset} = get_dir_details(dir)

    Logger.debug(inspect({seg_path, seg_size, offset}))

    fd = File.open!(seg_path, @write_mode)

    {:ok, idx_writer} = IndexWriter.start_link()

    {:ok,
     %{
       dir: dir,
       fd: fd,
       offset: offset,
       max_seg_size: opts[:max_seg_size],
       idx_writer: idx_writer,
       seg_path: seg_path,
       seg_size: seg_size
     }}
  end

  def append(pid, data) when is_pid(pid) do
    GenServer.call(pid, {:append, data})
  end

  def fsync(pid) when is_pid(pid) do
    GenServer.call(pid, :fsync)
  end

  def close(pid) when is_pid(pid) do
    GenServer.call(pid, :close)
  end

  # ---

  def handle_call(
        {:append, value},
        _from,
        %{
          dir: dir,
          fd: fd,
          offset: offset,
          max_seg_size: max_seg_size,
          idx_writer: idx_writer,
          seg_path: seg_path,
          seg_size: seg_size
        } = state
      ) do
    {fd, seg_path, seg_size} =
      if seg_size >= max_seg_size do
        File.close(fd)
        seg_path = Segment.seg_path(dir, offset)
        Logger.debug(fn -> "Segment split to: #{seg_path}" end)
        fd = File.open!(seg_path, @write_mode)
        {fd, seg_path, 0}
      else
        {fd, seg_path, seg_size}
      end

    data = value_to_iodata(value)
    {:ok, pos} = :file.position(fd, :cur)
    :ok = IO.binwrite(fd, data)

    IndexWriter.append(idx_writer, seg_path, pos)

    new_state = %{
      state
      | offset: offset + 1,
        seg_size: seg_size + byte_size(data),
        fd: fd,
        seg_path: seg_path
    }

    {:reply, {:ok, offset}, new_state}
  end

  def handle_call(:close, _from, %{fd: fd, idx_writer: idx_writer} = state) do
    IndexWriter.close(idx_writer)
    File.close(fd)
    {:reply, {:ok}, state}
  end

  def handle_call(:fsync, _from, %{fd: fd} = state) do
    {:reply, :file.datasync(fd), state}
  end

  # magic number
  @meta 0b00000000

  defp value_to_iodata(value) do
    # vsize:32
    # value: ?
    # timestamp: 64
    # crc: 32
    # meta: 8
    timestamp = :os.system_time(:millisecond)
    timestamp_data = <<timestamp::big-unsigned-integer-size(64)>>
    value_size = byte_size(value)
    value_size_data = <<value_size::big-unsigned-integer-size(32)>>
    meta_data = <<@meta::unsigned-integer-size(8)>>
    crc_data = <<:erlang.crc32(value)::big-unsigned-integer-size(32)>>

    <<value_size_data::binary, value::binary, timestamp_data::binary, meta_data::binary,
      crc_data::binary>>
  end

  defp get_dir_details(dir) do
    case Segment.greatest_seg(dir) do
      {seg_path, size} ->
        offset = IndexReader.greatest_offset(seg_path)
        {seg_path, size, offset}

      _ ->
        {Segment.zero_seg_path(dir), -1, 0}
    end
  end
end
