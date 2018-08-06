<?php

namespace Queueing;

/**
 * Class BulkPerformingException
 * @package Queueing
 * @deprecated - Use PerformingResult class instead
 */
class BulkPerformingException extends Exception
{
    use CreateFromExceptionTrait;

    /** @var BulkPerformingResult */
    private $result;

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
