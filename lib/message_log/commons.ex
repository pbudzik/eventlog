defmodule EventLog.Commons do
  @moduledoc """
  Common functions across modules.
  """
  @idx_ext ".idx"

  def idx_file_details(seg_path) do
    parts = Path.split(seg_path)

    dir =
      parts
      |> Enum.reverse()
      |> tl()
      |> Enum.reverse()
      |> Enum.join("/")

    base_offset = parts |> List.last() |> String.split(".") |> List.first()
    {base_offset |> String.to_integer(), Path.join(dir, base_offset <> @idx_ext)}
  end

  def temp_dir(), do: Path.join(System.tmp_dir!(), "_log")
end
