<?php declare(strict_types=1);
namespace PN\Redis;

/**
 * Wrapper around primitive i/o operations that transforms warnings into ErrorExceptions.
 */
class Io
{
  private $parentErrorHandler = null;

  public function __construct(
    /** @var resource $io */
    private readonly mixed $io,
  )
  {
  }

  private function handleError(int $severity, string $message, string $file, int $line): void
  {
    $throw = match ($severity) {
      E_ERROR, E_WARNING, E_DEPRECATED, E_NOTICE => true,
      default => false,
    };

    if ($throw) {
      throw new \ErrorException(
        message: $message,
        severity: $severity,
        filename: $file,
        line: $line,
      );
    }

    if ($this->parentErrorHandler !== null) {
      ($this->parentErrorHandler)($severity, $message, $file, $line);
    }
  }

  /**
   * @template T
   * @param \Closure(): T $closure
   * @return T
   * @throws \ErrorException
   */
  private function withErrorHandler(\Closure $closure): mixed
  {
    $this->parentErrorHandler = set_error_handler(self::handleError(...));

    try {
      return $closure();
    } finally {
      restore_error_handler();
      $this->parentErrorHandler = null;
    }
  }

  /** @throws \ErrorException */
  public function write(string $data): int
  {
    return $this->withErrorHandler(fn() => fwrite($this->io, $data));
  }

  public function read(int $length): ?string
  {
    return $this->withErrorHandler(function () use ($length) {
      $re = fread($this->io, $length);
      if ($re === false) {
        $re = null;
      }
      return $re;
    });
  }

  public function close(): void
  {
    $this->withErrorHandler(fn() => fclose($this->io));
  }

  public function gets(): ?string
  {
    return $this->withErrorHandler(function () {
      $re = fgets($this->io);
      if ($re === false) {
        return null;
      }
      return $re;
    });
  }

  public function flush(): void
  {
    $this->withErrorHandler(fn() => fflush($this->io));
  }

  public function streamSetTimeout(int $sec, int $usec): void
  {
    $this->withErrorHandler(fn() => stream_set_timeout($this->io, $sec, $usec));
  }

  public function tell(): int
  {
    return $this->withErrorHandler(fn() => ftell($this->io));
  }

  public function setOption(int $level, int $option, mixed $value): void
  {
    $this->withErrorHandler(function() use ($level, $option, $value) {
      $socket = socket_import_stream($this->io);
      socket_set_option($socket, $level, $option, $value);
    });
  }
}
