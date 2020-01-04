defmodule EventLog do
  @moduledoc """
  Can host multiple streams. Stream has a log and index.
  """
  use GenServer
  require Logger
  alias EventLog.{Appender, Reader}

  def start_link(dir, opts \\ []) do
    GenServer.start_link(__MODULE__, [dir, opts])
  end

  def init([dir, opts]) do
    File.mkdir_p!(dir)
    {:ok, %{dir: dir, opts: opts, streams: %{}}}
  end

  @doc """
  Creates a stream.
  """
  def create_stream(pid, name, opts \\ []) do
    GenServer.call(pid, {:create_stream, name, opts})
  end

  @doc """
  Deletes a stream.
  """
  def delete_stream(pid, name) do
    GenServer.call(pid, {:delete_stream, name})
  end

  @doc """
  Appends data entry to the end of the stream's log.
  """
  def append(pid, stream, data) do
    GenServer.call(pid, {:append, stream, data})
  end

  @doc """
  Basic info about underlying streams.
  """
  def info(pid) do
    GenServer.call(pid, :info)
  end

  @doc """
  Returns a reader that can read log. Initalized at a given offset, by default at the very
  beginning.
  """
  def get_reader(pid, stream, offset \\ 0) do
    GenServer.call(pid, {:get_reader, stream, offset})
  end

  @doc """
  Syncs all buffers to disk. Should be called if read is right after append.
  """
  def fsync(pid) do
    GenServer.call(pid, :fsync)
  end

  @doc """
  Closes all file descriptors.
  """
  def close(pid) do
    GenServer.call(pid, :close)
  end

  # ---

  def handle_call({:create_stream, name, _opts}, _from, %{dir: dir, streams: streams} = state) do
    if streams[name] do
      {:reply, {:error, :stream_exists}, state}
    else
      stream_dir = Path.join(dir, name)
      File.mkdir_p!(stream_dir)
      {:ok, appender} = Appender.start_link(stream_dir)
      stream = %{appender: appender, stream_dir: stream_dir}
      {:reply, {:ok, stream}, %{state | streams: Map.put(streams, name, stream)}}
    end
  end

  def handle_call({:delete_stream, name}, _from, %{streams: streams} = state) do
    case streams[name] do
      %{stream_dir: stream_dir} ->
        File.rm_rf!(stream_dir)
        {:reply, {:ok, 1}, Map.delete(state, name)}

      _ ->
        {:reply, {:error, :stream_not_exists}, state}
    end
  end

  def handle_call({:append, name, data}, _from, %{streams: streams} = state) do
    case streams[name] do
      %{appender: appender} ->
        Appender.append(appender, data)
        {:reply, {:ok, 1}, state}

      _ ->
        {:reply, {:error, :stream_not_exists}, state}
    end
  end

  def handle_call({:get_reader, name, offset}, _from, %{streams: streams} = state) do
    case streams[name] do
      %{stream_dir: stream_dir} ->
        {:ok, reader} = Reader.start_link(stream_dir, offset)
        {:reply, {:ok, reader}, state}

      _ ->
        {:reply, {:error, :stream_not_exists}, state}
    end
  end

  def handle_call(:info, _from, %{streams: streams} = state) do
    {:reply, {:ok, %{streams: streams |> Map.keys()}}, state}
  end

  def handle_call(:fsync, _from, %{streams: streams} = state) do
    for {_name, %{appender: appender}} <- streams do
      Appender.fsync(appender)
    end

    {:reply, :ok, state}
  end

  def handle_call(:close, _from, state) do
    {:reply, {:ok}, state}
  end
end
