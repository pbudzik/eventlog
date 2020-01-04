defmodule EventLog.Segment do
  @seg_ext ".seg"
  # for 64 bit integer
  @max_digits 20

  def greatest_seg_offset(dir) do
    case File.ls(dir) do
      {:ok, files} ->
        case files |> Enum.filter(&String.ends_with?(&1, @seg_ext)) do
          [] ->
            -1

          segs ->
            segs
            |> Enum.sort()
            |> List.last()
            |> String.split(".")
            |> List.first()
            |> String.to_integer()
        end

      _ ->
        :invalid_path
    end
  end

  def least_seg_offset(dir) do
    case File.ls(dir) do
      {:ok, files} ->
        case files |> Enum.filter(&String.ends_with?(&1, @seg_ext)) do
          [] ->
            -1

          segs ->
            segs
            |> Enum.sort()
            |> List.first()
            |> String.split(".")
            |> List.first()
            |> String.to_integer()
        end

      _ ->
        :invalid_path
    end
  end

  def greatest_seg_path(dir) do
    offset = greatest_seg_offset(dir)

    if offset >= 0 do
      Path.join(dir, seg_file_name(offset))
    else
      nil
    end
  end

  def least_seg_path(dir) do
    offset = least_seg_offset(dir)

    if offset >= 0 do
      Path.join(dir, seg_file_name(offset))
    else
      nil
    end
  end

  def greatest_seg_size(dir) do
    case greatest_seg_path(dir) do
      seg_path when not is_nil(seg_path) ->
        {:ok, stat} = File.stat(seg_path)
        stat.size

      _ ->
        -1
    end
  end

  def greatest_seg(dir) do
    case greatest_seg_path(dir) do
      seg_path when not is_nil(seg_path) ->
        {:ok, stat} = File.stat(seg_path)
        {seg_path, stat.size}

      _ ->
        nil
    end
  end

  def next_seg(dir) do
    file_name =
      case greatest_seg_offset(dir) do
        -1 -> seg_file_name(0)
        idx -> seg_file_name(idx + 1)
      end

    Path.join(dir, file_name)
  end

  def seg_file_name(offset) do
    o = "#{offset}"
    String.duplicate("0", @max_digits - String.length(o)) <> o <> @seg_ext
  end

  def path_offset(path),
    do:
      path
      |> Path.split()
      |> List.last()
      |> String.split(".")
      |> List.first()
      |> String.to_integer()

  def zero_seg_path(path), do: seg_path(path, 0)

  def seg_path(path, offset), do: Path.join(path, seg_file_name(offset))

  def next_seg_path(path) do
    parts = Path.split(path)

    dir =
      parts
      |> Enum.reverse()
      |> tl()
      |> Enum.reverse()
      |> Enum.join("/")

    seg = parts |> List.last()

    segs =
      File.ls!(dir)
      |> Enum.filter(&String.ends_with?(&1, @seg_ext))
      |> Enum.sort()
      |> List.to_tuple()

    case search(:next, segs, seg) do
      :not_found -> nil
      {:ok, :none} -> nil
      {:ok, next} -> Path.join(dir, next)
    end
  end

  @doc """
  Finds segment that contains given offset.
  """
  def find_offset_seg(dir, offset) do
    file = seg_file_name(offset)
    path = Path.join(dir, file)

    if File.exists?(path) do
      path
    else
      segs =
        ([file] ++ File.ls!(dir))
        |> Enum.filter(&String.ends_with?(&1, @seg_ext))
        |> Enum.sort()
        |> List.to_tuple()

      {:ok, seg_file} = search(:prev, segs, file)
      Path.join(dir, seg_file)
    end
  end

  def segments(dir),
    do:
      File.ls!(dir)
      |> Enum.filter(&String.ends_with?(&1, @seg_ext))

  def info(dir) do
    segments(dir)
    |> Enum.map(fn f ->
      stat = File.stat!(Path.join(dir, f))
      {Path.join(dir, f), {stat.size, stat.mtime}}
    end)
    |> Map.new()
  end

  ## binary search for segment which contains offset

  defp search(_d, {}, _key), do: :not_found

  defp search(d, t, key) when is_tuple(t) do
    do_search(d, t, key, 0, tuple_size(t))
  end

  defp do_search(_d, _t, _key, low, high) when high < low, do: :not_found

  defp do_search(d, t, key, low, high) do
    mid = div(high + low, 2)

    case elem(t, mid) do
      ^key ->
        case d do
          :next ->
            x = mid + 1

            if x > tuple_size(t) - 1 do
              {:ok, :none}
            else
              {:ok, elem(t, x)}
            end

          :prev ->
            x = mid - 1

            if x < 0 do
              {:ok, :none}
            else
              {:ok, elem(t, x)}
            end
        end

      v when key < v ->
        do_search(d, t, key, low, mid - 1)

      v when key > v ->
        do_search(d, t, key, mid + 1, high)
    end
  end
end
