
[![Version](https://img.shields.io/hexpm/v/event_log.svg?style=flat-square)](https://hex.pm/packages/event_log) 
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

# EventLog

EventLog is an indexed append only log written in Elixir. It stores data in log segments and each segment is indexed.
Entries of the log can be read sequentially either from begining or a given offset. 

## What is it for?

It's a building block component. Could help in EventSourcing/EventStore or distributed log implementation. 

## Key concepts

* Entries are being appended to log. There is no deletion operation.
* Entry contains the actual data, timestamp, crc and some meta data.
* Log is partitioned to segments that are named by it's least offset.
* Each segment has a coresponding index file which contains offset's position in segment.
* Log scales by splitting to more segments and has constant memory footprint.
* Stream is a log + index. 
* There are no NIFs and other dependencies (pure Elixir).

## Quickstart

* Add event_log to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:event_log, "~> 0.1.0"}
  ]
end
```

* Run:

    ```
    mix deps.get
    ```

## Usage

  ``` 
    {:ok, log} = EventLog.start_link(dir)
    {:ok, _} = EventLog.create_stream(log, "my_stream")
    EventLog.append(log, "my_stream", "foo0")
    EventLog.append(log, "my_stream", "foo1")
    EventLog.append(log, "my_stream", "foo2")
    # read from beginning
    {:ok, reader} = EventLog.get_reader(log, "my_stream")
    {:ok, {0, "foo0", _, _, _}} = Reader.get_next(reader)
    {:ok, {1, "foo1", _, _, _}} = Reader.get_next(reader)
    {:ok, {2, "foo2", _, _, _}} = Reader.get_next(reader)
    {:ok, :eof} = Reader.get_next(reader) # no more entries
    # read from offset=2
    {:ok, reader} = EventLog.get_reader(log, "my_stream", 2)
    {:ok, {2, "foo2", _, _, _}} = Reader.get_next(reader)
    entries = Reader.get_batch(reader, 0, 3)
    EventLog.close(log)
```
## Benchmarks

Run on my MacBook Pro:

```mix run benchmark/append.exs         
                                                                  
10000 messages of 128b size => elapsed: 1221 ms, mps: 8190 msg/s, throughput: 1.0483210483210483 MB/s
10000 messages of 512b size => elapsed: 1187 ms, mps: 8425 msg/s, throughput: 4.313395113732097 MB/s
10000 messages of 1024b size => elapsed: 1310 ms, mps: 7634 msg/s, throughput: 7.816793893129772 MB/s
```

## Contributing

There is a whole lot to do. If you want to help me, you are welcome. Please fork the repo, create a pull request against master, and be sure tests pass. 

## License

Apache Licence 2.0

http://www.apache.org/licenses/LICENSE-2.0


