defmodule EventLog.IndexReader do
  @moduledoc """
  Reads indices of a log.
  Every log segment (.seg file) has a coresponding index file (.idx).
  Index entry: {segment_file, position}
  """
  alias EventLog.{Utils, Commons, Segment}
  import Utils
  import Commons
  @read_mode [:binary, :read, :read_ahead]
  @entry_size 8

  @doc """
  Finds location(segment file and position) of a given log's offset
  """
  def offset_location(dir, offset) do
    seg_path = Segment.find_offset_seg(dir, offset)
    # if seg file starts with this offset, it's a hit
    if Path.join(dir, Segment.seg_file_name(offset)) == seg_path do
      {:ok, seg_path, 0}
    else
      find_pos(seg_path, offset)
    end
  end

  def greatest_offset(seg_path) do
    {_, idx_path} = idx_file_details(seg_path)
    round(file_size(idx_path) / @entry_size)
  end

  def is_index_ok?(dir) do
    segs = ls_ext(dir, ".seg")
    idxs = ls_ext(dir, ".idx")
    Enum.count(idxs) >= Enum.count(segs)
  end

  defp find_pos(seg_path, offset) do
    {base_offset, idx_path} = idx_file_details(seg_path)

    if File.exists?(idx_path) do
      fd = File.open!(idx_path, @read_mode)

      case get_pos(fd, offset, base_offset) do
        {:ok, pos} -> {:ok, seg_path, pos}
        {:error, :not_found} -> {:error, :not_found}
      end
    else
      {:error, :index_not_exists}
    end
  end

  defp get_pos(fd, offset, base_offset) do
    case read_entry(fd, offset - base_offset) do
      {:ok, pos} -> {:ok, pos}
      _ -> {:error, :not_found}
    end
  end

  defp read_entry(fd, offset) do
    pos = offset * @entry_size

    case :file.pread(fd, pos, 8) do
      :eof -> :eof
      {:ok, pos_data} -> {:ok, :binary.decode_unsigned(pos_data)}
    end
  end
end
