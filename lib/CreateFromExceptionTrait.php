<?php

namespace Queueing;

trait CreateFromExceptionTrait
{
    public static function createFromException(\Throwable $e): self
    {
        return (new self($e->getMessage(), intval($e->getCode()), $e));
    }
}
