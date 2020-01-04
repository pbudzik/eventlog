defmodule EventLog.Utils do
  @moduledoc """
  Common utilities.
  """
  def file_size(file) do
    {:ok, info} = :file.read_file_info(file)
    elem(info, 1)
  end

  def random_string(len) do
    :crypto.strong_rand_bytes(len)
    |> Base.url_encode64()
    |> binary_part(0, len)
  end

  def ls_ext(dir, ext), do: File.ls!(dir) |> Enum.filter(&String.ends_with?(&1, ext))
end
