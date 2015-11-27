Fluentd output plugin to send data to flume use thrift.

## fluentd configuration

define a output like:

```
type flume
<server>
    host host1
    port 41417
</server>
<server>
    host host2
    port 41417
</server>
```

This plugin will do round robin amoung hosts, and will add `host`, `hostname`, `tag` in message header.

## flume configuration

define a source like:

```
a1.sources.avro-source2.channels = ch2
a1.sources.avro-source2.type = thrift
a1.sources.avro-source2.bind = 0.0.0.0
a1.sources.avro-source2.port = 41417
```

Tested on flume 1.5.2 and 1.6.
