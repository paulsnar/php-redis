<?php declare(strict_types=1);

namespace PN\Redis;

class Status
{
  public $content;

  public function __construct(string $s)
  {
    $this->content = $s;
  }

  public function __toString()
  {
    return $this->content;
  }
}

