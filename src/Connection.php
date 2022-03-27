<?php declare(strict_types=1);

namespace PN\Redis;

class Connection
{
  protected $s, $sub = false;

  public function __construct(array $opts = [ ])
  {
    $host = $opts['host'] ?? '127.0.0.1';
    $port = $opts['port'] ?? 6379;
    $timeout = $opts['timeout'] ?? 1;

    $s = $this->s = stream_socket_client(
      "tcp://{$host}:{$port}", $errno, $errstr, $timeout);
    if ($s === false) {
      throw new \RuntimeException("Redis connection failed: {$errstr}", $errno);
    }

    stream_set_timeout($s, $timeout);

    if (function_exists('socket_import_stream')) {
      $sock = socket_import_stream($s);
      socket_set_option($sock, \SOL_TCP, \TCP_NODELAY, 1);
    }
  }

  public function __destruct()
  {
    fclose($this->s);
  }

  public function setTimeout($sec, $msec = 0)
  {
    return stream_set_timeout($this->s, $sec, $msec);
  }

  protected function write(array $args)
  {
    $argc = count($args);
    $buf = "*{$argc}\r\n";

    foreach ($args as $arg) {
      $argl = strlen($arg);
      $buf .= "\${$argl}\r\n{$arg}\r\n";
    }

    fwrite($this->s, $buf);
    fflush($this->s);
  }

  protected function read()
  {
    $chunk = fgets($this->s);
    if ($chunk === false || $chunk === '') {
      throw new \Exception('REPL protocol failure: empty chunk');
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
            throw new \Exception('REPL protocol failure: empty chunk');
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
      "REPL protocol failure: unknown response type: {$prefix}");
  }

  public function call(string $command, ...$args)
  {
    // Flatten any keyword args if present.
    $flatArgs = [strtoupper($command)];
    foreach ($args as $key => $arg) {
      if (is_string($key)) {
        array_push($flatArgs, $key, (string) $arg);
      } else {
        array_push($flatArgs, (string) $arg);
      }
    }

    $this->write($flatArgs);
    return $this->read();
  }
}
