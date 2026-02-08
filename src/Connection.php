<?php declare(strict_types=1);

namespace PN\Redis;

class Connection
{
  protected function __construct(
    /** @var resource $s */
    protected readonly mixed $s,
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

    if (extension_loaded('sockets')) {
      $sock = socket_import_stream($s);
      socket_set_option($sock, \SOL_TCP, \TCP_NODELAY, 1);
    }

    $self = new static($s, close: true);
    $self->setTimeout($timeout);
    return $self;
  }

  public static function persistent(string $host = '127.0.0.1', int $port = 6379, float $timeout = 1.0): static
  {
    $s = pfsockopen("tcp://{$host}", $port, $errno, $errstr, $timeout);
    if ($s === false) {
      throw new \RuntimeException("Redis connection failed: {$errstr}", $errno);
    }

    $reused = ftell($s) !== 0;

    $self = new static($s, close: false);
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
        $self = new static($s, close: false);
      }
    }

    $self->setTimeout($timeout);
    return $self;
  }

  public function __destruct()
  {
    if ($this->close) {
      fclose($this->s);
    }
  }

  public function setTimeout(float $timeout): void
  {
    $sec = (int)$timeout;
    $usec = (int)floor(($timeout * 1e6) % 1e6);
    if (stream_set_timeout($this->s, $sec, $usec) === false) {
      throw new \RuntimeException("Failed to set stream timeout");
    }
  }

  protected function write(array $args): void
  {
    $argc = count($args);
    $buf = "*{$argc}\r\n";

    foreach ($args as $arg) {
      $argl = strlen($arg);
      $buf .= "\${$argl}\r\n{$arg}\r\n";
    }

    $re = fwrite($this->s, $buf);
    if ($re !== strlen($buf)) {
      throw new \RuntimeException("Failed to write");
    }
    fflush($this->s);
  }

  protected function read(): null|int|string|array|Status
  {
    $chunk = fgets($this->s);
    if ($chunk === false || $chunk === '') {
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
          $chunk = fread($this->s, min($remaining, 8192));
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
        array_push($flatArgs, (string) $arg);
      }
    }

    $this->write($flatArgs);
    return $this->read();
  }
}
