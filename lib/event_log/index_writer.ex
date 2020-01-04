defmodule EventLog.IndexWriter do
  @moduledoc """
  Writes offset index of a given log.

  Every log segment (.seg file) has a coresponding index file (.idx).

  Index entry: {segment_file, position}
  """
  use GenServer
  require Logger
  import EventLog.Commons

  @write_mode [:append, :binary]
  #@write_mode [:append, :binary,:delayed_write]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(_opts) do
    {:ok, %{idx: %{}}}
  end

  @doc """
  Appends index entry. Index entry: {segment_file, position}
  """
  def append(pid, seg_path, pos) when is_pid(pid) do
    GenServer.cast(pid, {:append, seg_path, pos})
  end

  @doc """
  Closes index writer
  """
  def close(pid) when is_pid(pid) do
    GenServer.call(pid, :close)
  end

  # ---

  def handle_cast({:append, seg_path, pos}, %{idx: idx} = state) do
    case idx[seg_path] do
      fd when not is_nil(fd) ->
        append_idx_entry(fd, pos)
        {:noreply, state}

      _ ->
        close_idx(idx)

        {_, idx_path} = idx_file_details(seg_path)
        fd = File.open!(idx_path, @write_mode)
        append_idx_entry(fd, pos)
        {:noreply, %{state | idx: %{seg_path: fd}}}
    end
  end

  def handle_call(:close, _from, %{idx: idx} = state) do
    close_idx(idx)
    {:reply, {:ok}, state}
  end

  defp append_idx_entry(fd, pos) do
    :ok = IO.binwrite(fd, <<pos::big-unsigned-integer-size(64)>>)
  end

  defp close_idx(idx) do
    for fd <- idx |> Map.values() do
      File.close(fd)
    end
  end
end
