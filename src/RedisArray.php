<?php declare(strict_types=1);

namespace PN\Redis;

class RedisArray implements \ArrayAccess, \Countable, \Iterator
{
  protected $list, $keyvalue;
  private $iter;

  public function __construct(array $list)
  {
    $this->list = $list;
    foreach ($this->list as &$value) {
      if (is_array($value)) {
        $value = new RedisArray($value);
      }
    }
  }

  public function toArray()
  {
    return $this->list;
  }

  public function toAssocArray()
  {
    if ($this->keyvalue !== null) {
      return $this->keyvalue;
    }

    if (count($this->list) % 2 !== 0) {
      throw new \LogicException('Array cannot be interpreted as keyvalue');
    }

    $kv = [ ];
    $l = count($this->list);
    for ($i = 0; $i < $l; $i += 2) {
      $key = $this->list[$i];
      $value = $this->list[$i + 1];
      $kv[$key] = $value;
    }

    $this->keyvalue = $kv;
    return $kv;
  }

  //#pragma mark ArrayAccess

  public function offsetExists($key)
  {
    if (is_string($key)) {
      if ($this->keyvalue === null) {
        $kv = $this->toAssocArray();
        return array_key_exists($key, $kv);
      }
      return array_key_exists($key, $this->keyvalue);
    }
    return array_key_exists($key, $this->list);
  }

  public function offsetGet($key)
  {
    if (is_string($key)) {
      if ($this->keyvalue === null) {
        $kv = $this->toAssocArray();
        return $kv[$key] ?? null;
      }
      return $this->keyvalue[$key] ?? null;
    }
    return $this->list[$key] ?? null;
  }

  public function offsetUnset($key)
  {
    throw new \LogicException('Cannot modify Redis array');
  }

  public function offsetSet($key, $value)
  {
    throw new \LogicException('Cannot modify Redis array');
  }

  //#pragma mark Countable

  public function count()
  {
    return count($this->list);
  }

  //#pragma mark Iterator

  public function rewind()
  {
    $this->iter = (function ($list) {
      foreach ($list as $key => $value) {
        yield $key => $value;
      }
    })($this->list);

    return $this->iter->rewind();
  }

  public function key() { return $this->iter->key(); }
  public function current() { return $this->iter->current(); }
  public function next() { return $this->iter->next(); }
  public function valid() { return $this->iter->valid(); }
}
