<?php

namespace Queueing;

class BulkPerformingException extends Exception
{
    /** @var \Throwable */
    private $e;
    /** @var BulkPerformingResult */
    private $result;

    public static function createFromException(\Throwable $e): self
    {
         return (new self($e->getMessage(), intval($e->getCode()), $e))->setSrcException($e);
    }

    /**
     * @return \Throwable
     */
    public function getSrcException(): \Throwable
    {
        return $this->e;
    }

    /**
     * @param \Throwable $e
     * @return self
     */
    public function setSrcException(\Throwable $e): self
    {
        $this->e = $e;
        return $this;
    }

    /**
     * @return BulkPerformingResult
     */
    public function getResult(): BulkPerformingResult
    {
        return $this->result;
    }

    /**
     * @param BulkPerformingResult $result
     * @return self
     */
    public function setResult(BulkPerformingResult $result): self
    {
        $this->result = $result;
        return $this;
    }
}
