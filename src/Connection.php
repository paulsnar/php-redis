<?php declare(strict_types=1);

namespace PN\Redis;

class Connection
{
  protected function __construct(
    protected readonly Io $io,
    protected readonly bool $close = true,
  )
  {
  }

  public static function connect(string $host = '127.0.0.1', int $port = 6379, float $timeout = 1.0): static
  {
    $s = stream_socket_client("tcp://{$host}:{$port}", $errno, $errstr, $timeout);
    if ($s === false) {
      throw new \RuntimeException("Redis connection failed: {$errstr}", $errno);
    }

    $io = new Io($s);

    if (extension_loaded('sockets')) {
      $io->setOption(\SOL_TCP, \TCP_NODELAY, 1);
    }

    $self = new static($io, close: true);
    $self->setTimeout($timeout);
    return $self;
  }

  public static function persistent(string $host = '127.0.0.1', int $port = 6379, float $timeout = 1.0): static
  {
    $s = pfsockopen("tcp://{$host}", $port, $errno, $errstr, $timeout);
    if ($s === false) {
      throw new \RuntimeException("Redis connection failed: {$errstr}", $errno);
    }

    $io = new Io($s);

    $reused = $io->tell() !== 0;

    $self = new static($io, close: false);
    if ($reused) {
      try {
        $re = $self->call('PING');
        assert($re instanceof Status && $re->content === 'PONG');
      } catch (\Exception $exc) {
        fclose($s);
        $s = pfsockopen("tcp://{$host}", $port, $errno, $errstr, $timeout);
        if ($s === false) {
          throw new \RuntimeException("Redis connection failed: {$errstr}", $errno, previous: $exc);
        }
        $io = new Io($s);
        $self = new static($io, close: false);
      }
    }

    $self->setTimeout($timeout);
    return $self;
  }

  public function __destruct()
  {
    if ($this->close) {
      $this->io->close();
    }
  }

  public function setTimeout(float $timeout): void
  {
    $sec = (int)$timeout;
    $usec = (int)floor(($timeout * 1e6) % 1e6);
    $this->io->streamSetTimeout($sec, $usec);
  }

  protected function write(array $args): void
  {
    $argc = count($args);
    $buf = "*{$argc}\r\n";

    foreach ($args as $arg) {
      $argl = strlen($arg);
      $buf .= "\${$argl}\r\n{$arg}\r\n";
    }

    $this->io->write($buf);
    $this->io->flush();
  }

  protected function read(): null|int|string|array|Status
  {
    $chunk = $this->io->gets();
    if ($chunk === null) {
      throw new \Exception("Connection closed");
    } else if ($chunk === '') {
      throw new \Exception('RESP protocol failure: empty chunk');
    }

    $prefix = $chunk[0];
    $payload = substr($chunk, 1, -2);

    switch ($prefix) {
      case '+':
        return new Status($payload);

      case '-':
        throw new ServerError($payload);

      case ':':
        $i = (int) $payload;
        $i = $i == $payload ? $i : $payload;
        return $i;

      case '$':
        $size = (int) $payload;
        if ($size === -1) {
          return null;
        }

        $bulk = '';
        $remaining = ($size += 2);

        do {
          $chunk = $this->io->read(min($remaining, 8192));
          if ($chunk === false || $chunk === '') {
            throw new \Exception('RESP protocol failure: empty chunk');
          }

          $bulk .= $chunk;
          $remaining -= strlen($chunk);
        } while ($remaining > 0);
        return substr($bulk, 0, -2);

      case '*':
        $count = (int) $payload;
        if ($count === -1) {
          return null;
        }

        $a = [ ];
        for ($i = 0; $i < $count; $i += 1) {
          $a[$i] = $this->read();
        }
        return $a;
    }

    throw new \Exception(
      "RESP protocol failure: unknown response type: {$prefix}");
  }

  public function call(string $command, ...$args): null|int|string|array|Status
  {
    // Flatten any keyword args if present.
    $flatArgs = [strtoupper($command)];
    foreach ($args as $key => $arg) {
      if (is_string($key)) {
        array_push($flatArgs, strtoupper($key), (string) $arg);
      } else {
        $flatArgs[] = (string)$arg;
      }
    }

    $this->write($flatArgs);
    return $this->read();
  }
}
