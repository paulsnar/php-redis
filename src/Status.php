<?php declare(strict_types=1);

namespace PN\Redis;

readonly class Status
{
  public function __construct(
    public string $content,
  )
  {
  }

  public function __toString()
  {
    return $this->content;
  }
}
