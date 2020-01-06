defmodule EventLog.Reader do
  @moduledoc """
  Reads entries of a log.
  """
  use GenServer
  require Logger
  alias EventLog.{Segment, IndexReader}

  @read_mode [:read, :binary, :read_ahead]

  @doc """
  Starts a log reader.
  """
  def start_link(dir, offset \\ 0) do
    GenServer.start_link(__MODULE__, {dir, offset})
  end

  def init({dir, offset}) do
    {:ok, fd, seg_path} =
      if offset == 0 do
        open_least_segment(dir)
      else
        open_segment_at_offset(dir, offset)
      end

    if not IndexReader.is_index_ok?(dir) do
      Logger.error("Missing .idx files")
    end

    # TODO: print info
    {:ok, %{fd: fd, path: seg_path, dir: dir, offset: offset}}
  end

  @doc """
  Fetches next log entry. If no more entries it returns :eof.
  """
  def get_next(pid) when is_pid(pid) do
    GenServer.call(pid, :get_next)
  end

  @doc """
  Fetches up to `limit` entries starting at given offset.
  """
  def get_batch(pid, offset, limit) when is_pid(pid) do
    GenServer.call(pid, {:seek, offset})
    do_get_next(pid, limit, [], 0) |> Enum.reverse()
  end

  @doc """
  Fetches one entry at given offset.
  """
  def get_one(pid, offset) when is_pid(pid) do
    case get_batch(pid, offset, 1) do
      [entry] -> {:ok, entry}
      _ -> {:ok, nil}
    end
  end

  @doc """
  Sets log offset position to given value.
  """
  def seek(_pid, offset) when offset < 0, do: {:error, :invalid_offset}

  @doc """
  Sets log offset position to given value.
  """

  def seek(pid, offset) when is_pid(pid) do
    GenServer.call(pid, {:seek, offset})
  end

  @doc """
  Closes log reader.
  """
  def close(pid) when is_pid(pid) do
    GenServer.call(pid, :close)
  end

  # ----

  defp do_get_next(pid, limit, acc, i) do
    if i >= limit do
      acc
    else
      case get_next(pid) do
        {:ok, :eof} -> acc
        {:ok, entry} -> do_get_next(pid, limit, [entry | acc], i + 1)
      end
    end
  end

  def handle_call(:get_next, _from, %{fd: fd, path: path, offset: offset} = state) do
    case read_next_entry(fd, path, offset) do
      {:ok, {entry, fd, path, new_offset}} ->
        {:reply, {:ok, entry}, %{state | fd: fd, path: path, offset: new_offset}}

      {:ok, :eof} ->
        {:reply, {:ok, :eof}, state}
    end
  end

  def handle_call({:seek, offset}, _from, %{fd: fd, dir: dir} = state) do
    case open_segment_at_offset(dir, offset) do
      {:ok, new_fd, seg_path} ->
        File.close(fd)
        {:reply, {:ok, offset}, %{state | fd: new_fd, path: seg_path, offset: offset}}

      _ ->
        {:reply, {:error, :invalid_offset}, state}
    end
  end

  def handle_call(:close, _from, %{fd: fd} = state) do
    {:reply, File.close(fd), state}
  end

  # vsize:32/4
  # value: ?
  # timestamp: 64/8
  # crc: 32/4
  # meta: 8/1

  defp read_next_entry(fd, path, offset) do
    vsize_data = IO.binread(fd, 4)

    if vsize_data == {:error, :terminated} or vsize_data == :eof do
      File.close(fd)
      new_path = Segment.next_seg_path(path)

      if new_path do
        Logger.debug(fn -> "Switching to next segment: #{inspect(new_path)}" end)
        new_fd = File.open!(new_path, @read_mode)
        read_next_entry(new_fd, new_path, offset)
      else
        {:ok, :eof}
      end
    else
      vsize = :binary.decode_unsigned(vsize_data)
      data = IO.binread(fd, vsize)
      ts = IO.binread(fd, 8)
      meta = IO.binread(fd, 1)
      crc = IO.binread(fd, 4)

      entry =
        {offset, :erlang.binary_to_term(data), :binary.decode_unsigned(ts),
         :binary.decode_unsigned(meta), :binary.decode_unsigned(crc)}

      {:ok, {entry, fd, path, offset + 1}}
    end
  end

  defp open_least_segment(dir) do
    seg_path = Segment.least_seg_path(dir)
    fd = File.open!(seg_path, @read_mode)
    {:ok, fd, seg_path}
  end

  defp open_segment_at_offset(dir, offset) do
    case IndexReader.offset_location(dir, offset) do
      {:ok, seg_path, pos} ->
        fd = File.open!(seg_path, @read_mode)

        {:ok, ^pos} = :file.position(fd, pos)
        {:ok, fd, seg_path}

      {:error, _} ->
        {:error, :invalid_offset}
    end
  end
end
